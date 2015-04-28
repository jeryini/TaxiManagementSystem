package com.jernejerin.traffic.architectures;

import com.jernejerin.traffic.entities.Trip;
import com.jernejerin.traffic.helper.PollingDriver;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.Environment;
import reactor.fn.Consumer;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.ChannelStream;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.broadcast.Broadcaster;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Jernej Jerin on 13.4.2015.
 */
public class EDA {
    private final static int PORT = 30000;
    private final static int PROCS = Runtime.getRuntime().availableProcessors();
    private final static Logger LOGGER = Logger.getLogger(EDA.class.getName());

    public static void main(String[] args) throws InterruptedException, Exception {
        LOGGER.log(Level.INFO, "Starting EDA server from thread = " + Thread.currentThread());
        // environment initialization
        Environment.initializeIfEmpty().assignErrorJournal();

        // event driven broadcaster. We use a hot stream as we have a unbounded
        // stream of data incoming.
        Broadcaster<String> trips = Broadcaster.create(Environment.get());

        // TCP server
        TcpServer<String, String> server = NetStreams.tcpServer(
                spec -> spec
                        .listen(PORT)
                        .dispatcher(Environment.cachedDispatcher())
                        .codec(StandardCodecs.STRING_CODEC)
        );

        // Then we set up and register the PoolingDriver.
        LOGGER.log(Level.INFO, "Registering pooling driver from thread = " + Thread.currentThread());
        try {
            PollingDriver.setupDriver("jdbc:mysql://localhost:3307/taxi_trip_management");
        } catch (Exception e) {
            e.printStackTrace();
        }


        // consumer for TCP server
        server.log("server").consume(new Consumer<ChannelStream<String, String>>() {
            @Override
            public void accept(ChannelStream<String, String> channel) {
                channel.log("channel").consume(new Consumer<String>() {
                    @Override
                    public void accept(String trip) {
                        LOGGER.log(Level.INFO, "TCP server receiving trip " +
                                trip + " from thread = " + Thread.currentThread());
                        // dispatch event to a broadcaster pipeline, which uses the same number of threads
                        // as there are cores
                        trips.onNext(trip);
                        LOGGER.log(Level.INFO, "TCP server send ticket to streaming pipeline for ticket = " +
                                trip + " from thread = " + Thread.currentThread());
                    }
                });
            }
        });

        // processing through streams, where number of threads is the same as number of cores
        trips.log("broadcaster")
            .map(t -> {
                LOGGER.log(Level.INFO, "Distributing for ticket = " +
                        t + " from thread = " + Thread.currentThread());
                return t;
            })
            // paralellize stream tasks to threads
            .groupBy(t -> t.hashCode() % PROCS)
            .consume(stream -> {
                // Read this: https://groups.google.com/d/msg/reactor-framework/JO0hGftOaZs/20IhESjPQI0J
                // Also: https://groups.google.com/forum/#!searchin/reactor-framework/findOne/reactor-framework/ldOfjEsQzio/MeLVWhrCDOAJ
                stream.dispatchOn(Environment.newCachedDispatchers(PROCS).get())
                    // parsing and validating trip structure
                    .map(t -> {
                        // parse, validate and return ticket object
                        Trip trip = TripOperations.parseValidateTrip(t);

                        // pass forward trip as new event
                        return trip;
                    })
                            // I/O intensive operations
                    .map(t -> {
                        // save ticket
                        TripOperations.insertTrip(t);
                        return t;
                    })
                    // TODO (Jernej Jerin): Add CPU intensive task for query 1: Frequent routes
                    .map(bt -> {
                        return bt;
                    })
                    // TODO (Jernej Jerin): Add CPU intensive task for query 2: Profitable areas
                    .map(bt -> {
                        return bt;
                    })
                    .consume(bt -> {
                    });
            });

        server.start().await();
        Thread.sleep(Long.MAX_VALUE);
    }
}
