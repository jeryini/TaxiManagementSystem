package com.jernejerin.traffic.architectures;

import com.jernejerin.traffic.database.PollingDriver;
import com.jernejerin.traffic.database.TicketOperations;
import com.jernejerin.traffic.entities.Trip;
import reactor.Environment;
import reactor.core.DispatcherSupplier;
import reactor.fn.Consumer;
import reactor.io.codec.StandardCodecs;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.ChannelStream;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.broadcast.Broadcaster;

import java.math.BigInteger;
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
            PollingDriver.setupDriver("jdbc:mysql://localhost:3307/traffic_management");
        } catch (Exception e) {
            e.printStackTrace();
        }


        // consumer for TCP server
        server/*.log("server")*/.consume(new Consumer<ChannelStream<String, String>>() {
            @Override
            public void accept(ChannelStream<String, String> channel) {
                channel/*.log("channel")*/.consume(new Consumer<String>() {
                    @Override
                    public void accept(String trip) {
                        LOGGER.log(Level.INFO, "TCP server receiving ticket " +
                                trip + " from thread = " + Thread.currentThread());
                        // dispatch event to a broadcaster pipeline,
                        // which uses the same number of threads
                        // as there are cores
//                        trip.setStartProcessing(System.currentTimeMillis());
                        trips.onNext(trip);
//                        LOGGER.log(Level.INFO, "TCP server send ticket to streaming pipeline for ticket id = " +
//                                trip.getId() + " from thread = " + Thread.currentThread());
                    }
                });
            }
        });

        int count = 0;
        // processing through streams, where number of threads is the same as number of cores
        trips/*.log("broadcaster")*/
                .map(bt -> {
//                    LOGGER.log(Level.INFO, "Distributing for ticket id = " +
//                            bt.getId() + " from thread = " + Thread.currentThread());
                    return bt;
                })
                // paralellize stream tasks to threads
                .groupBy(s -> s.hashCode() % PROCS)
                .consume(stream -> {
                    // Read this: https://groups.google.com/d/msg/reactor-framework/JO0hGftOaZs/20IhESjPQI0J
                    // Also: https://groups.google.com/forum/#!searchin/reactor-framework/findOne/reactor-framework/ldOfjEsQzio/MeLVWhrCDOAJ
                    stream.dispatchOn(Environment.newCachedDispatchers(PROCS).get())
                        // validate ticket structure, which means checking that values are inside specified interval
                        .map(bt -> {
                            // validate and return fixed ticket
//                            bt = TicketOperations.validateTicketValues(bt);

                            // pass forward ticket as new event
                            return bt;
                        })
//                        // I/O intensive operations
//                        .map(bt -> {
//                            // save ticket
//                            TicketOperations.insertTicket(bt);
//
//                            // get vehicle if it exists
////                            Vehicle vehicle = TicketOperations
//                            return bt;
//                        })
//                        // TODO (Jernej Jerin): We need CPU intensive task here. Computing
//                        // TODO (Jernej Jerin): statistics is not really that CPU intensive.
//                        // TODO (Jernej Jerin): I propose finding a primer or computing large factorials.
//                        .map(bt -> {
//                            BigInteger inc = new BigInteger("1");
//                            BigInteger fact = new BigInteger("1");
//
//                            for (int c = 1; c <= 1000; c++) {
//                                fact = fact.multiply(inc);
//                                inc = inc.add(BigInteger.ONE);
//                            }
////                            LOGGER.log(Level.INFO, "Distributing for ticket id = " +
////                                    bt.getId() + " from thread = " + Thread.currentThread());
////                            bt.setFinishProcessing(System.currentTimeMillis());
//                            return bt;
//                        })
                        .consume(bt -> {
//                            TicketOperations.updateTicket(bt);
                        });
                });

        server.start().await();
        Thread.sleep(Long.MAX_VALUE);
    }
}
