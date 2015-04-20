package com.jernejerin.traffic.architectures;

import com.jernejerin.traffic.database.PollingDriver;
import com.jernejerin.traffic.database.TicketOperations;
import com.jernejerin.traffic.entities.BaseTicket;
import reactor.Environment;
import reactor.core.DispatcherSupplier;
import reactor.fn.Consumer;
import reactor.io.codec.json.JsonCodec;
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
        Broadcaster<BaseTicket> broadcaster = Broadcaster.create(Environment.get());

        // codec for BaseTicket class
        JsonCodec<BaseTicket, BaseTicket> codec = new JsonCodec<BaseTicket, BaseTicket>(BaseTicket.class);

        DispatcherSupplier supplier2 = Environment.newCachedDispatchers(5, "pool2");

        // TCP server
        TcpServer<BaseTicket, BaseTicket> server = NetStreams.tcpServer(
                spec -> spec
                        .listen(PORT)
                        .codec(codec)
                        .dispatcher(supplier2.get())
        );

        // Then we set up and register the PoolingDriver.
        LOGGER.log(Level.INFO, "Registering pooling driver from thread = " + Thread.currentThread());
        try {
            PollingDriver.setupDriver("jdbc:mysql://localhost:3307/traffic_management");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // consumer for TCP server
        server./*log("server").*/consume(new Consumer<ChannelStream<BaseTicket, BaseTicket>>() {
            @Override
            public void accept(ChannelStream<BaseTicket, BaseTicket> channel) {
                channel./*log("channel").*/consume(new Consumer<BaseTicket>() {
                    @Override
                    public void accept(BaseTicket baseTicket) {
                        LOGGER.log(Level.INFO, "TCP server receiving ticket with id = " +
                                baseTicket.getId() + " from thread = " + Thread.currentThread());
                        // dispatch event to a broadcaster pipeline,
                        // which uses the same number of threads
                        // as there are cores
                        broadcaster.onNext(baseTicket);
                        LOGGER.log(Level.INFO, "TCP server send ticket to streaming pipeline for ticket id = " +
                                baseTicket.getId() + " from thread = " + Thread.currentThread());
                    }
                });
            }
        });

        // processing through streams, where number of threads is the same as number of cores
        broadcaster.
                map(bt -> {
                    LOGGER.log(Level.INFO, "Distributing for ticket id = " +
                            bt.getId() + " from thread = " + Thread.currentThread());
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
                            bt = TicketOperations.validateTicketValues(bt);

                            // pass forward ticket as new event
                            return bt;
                        })
                        // insert ticket to DB using prepared statement
                        .map(bt -> {
                            // save ticket
                            TicketOperations.insertTicket(bt);
                            return bt;
                        })
                        // compute statistics
                        .consume(b -> {

                        });
                });

        server.start().await();
        Thread.sleep(Long.MAX_VALUE);
    }
}
