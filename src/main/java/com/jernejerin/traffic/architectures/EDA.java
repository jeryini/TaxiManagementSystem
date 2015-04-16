package com.jernejerin.traffic.architectures;

import com.jernejerin.traffic.entities.BaseTicket;
import reactor.Environment;
import reactor.fn.Consumer;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.ChannelStream;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.broadcast.Broadcaster;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Jernej Jerin on 13.4.2015.
 */
public class EDA {
    private final static int port = 30000;
    private final static int procs = Runtime.getRuntime().availableProcessors();
    private final static Logger LOGGER = Logger.getLogger(EDA.class.getName());

    public static void main(String[] args) throws InterruptedException, Exception {
        // environment initialization
        Environment.initializeIfEmpty().assignErrorJournal();

        // event driven broadcaster
        Broadcaster<BaseTicket> broadcaster = Broadcaster.create(Environment.get());

        // codec for BaseTicket class
        JsonCodec<BaseTicket, BaseTicket> codec = new JsonCodec<BaseTicket, BaseTicket>(BaseTicket.class);

        // get prepared statement for inserting ticket into database
        PreparedStatement insertTicket = getInsertPreparedStatement();
        if (insertTicket == null)
            throw new Exception();

        // TCP server
        TcpServer<BaseTicket, BaseTicket> server = NetStreams.tcpServer(
                spec -> spec
                        .listen(port)
                        .codec(codec)
                        .dispatcher(Environment.cachedDispatcher())
        );

        // consumer for TCP server
        server.log("server").consume(new Consumer<ChannelStream<BaseTicket, BaseTicket>>() {
            @Override
            public void accept(ChannelStream<BaseTicket, BaseTicket> channel) {
                channel.log("channel").consume(new Consumer<BaseTicket>() {
                    @Override
                    public void accept(BaseTicket baseTicket) {
                        System.out.printf("TCP server receiving ticket %s, speed %s from thread %s%n", baseTicket.getId(), baseTicket.getSpeed(), Thread.currentThread());
                        // dispatch event to a broadcaster pipeline,
                        // which uses the same number of threads
                        // as there are cores
                        broadcaster.onNext(baseTicket);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });

        // processing through streams, where number of threads is the same as number of cores
        broadcaster.
                map(bt -> {
                    System.out.printf("Distributing from thread %s%n", Thread.currentThread());
                    return bt;
                })
                // paralellize stream tasks to threads
                .groupBy(s -> s.hashCode() % procs)
                .consume(stream -> {
                    // Read this: https://groups.google.com/d/msg/reactor-framework/JO0hGftOaZs/20IhESjPQI0J
                    // Also: https://groups.google.com/forum/#!searchin/reactor-framework/findOne/reactor-framework/ldOfjEsQzio/MeLVWhrCDOAJ
                    stream.dispatchOn(Environment.newCachedDispatchers(procs).get())
                        // validate ticket structure
                        .map(bt -> bt)
                                // insert ticket to DB using prepared statement
                        .map(bt -> {
                            System.out.printf("Insert ticket %s into DB from thread %s", bt.getId(), Thread.currentThread());
                            try {
                                // TODO (Jernej Jerin): Find out why we get duplicate values.
                                System.out.printf("Insert ticket %s into DB from thread %s", bt.getId(), Thread.currentThread());
                                insertTicket.setInt(1, bt.getId());
                                insertTicket.setLong(2, bt.getStartTime());
                                insertTicket.setLong(3, bt.getLastUpdated());
                                insertTicket.setDouble(4, bt.getSpeed());
                                insertTicket.setInt(5, bt.getCurrentLaneId());
                                insertTicket.setInt(6, bt.getPreviousSectionId());
                                insertTicket.setInt(7, bt.getNextSectionId());
                                insertTicket.setDouble(8, bt.getSectionPositon());
                                insertTicket.setInt(9, bt.getDestinationId());
                                insertTicket.setInt(10, bt.getVehicleId());

                                insertTicket.execute();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }

                            // pass forward base ticket
                            return bt;
                        })
                        // compute statistics
                        .consume(b -> System.out.printf("Processing from thread %s%n", Thread.currentThread()));
                });

        server.start().await();

        // run the server forever
        // TODO(Jernej Jerin): Is there a better way to do this?
        Thread.sleep(Long.MAX_VALUE);
    }

    private static PreparedStatement getInsertPreparedStatement() {
        PreparedStatement insertStatement = null;
        try {
            // create a mysql database connection
            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://localhost:3307/traffic_management";

            // MySQL connection settings
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, "root", "jrj18ene9891");

            // insert statement for traffic ticket
            String query = "insert into ticket (id, startTime, lastUpdated, speed, " +
                    "currentLaneId, previousSectionId, nextSectionId, sectionPosition, " +
                    "destinationId, vehicleId) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            // create the mysql insert prepared statement and return it
            insertStatement = conn.prepareStatement(query);
        } catch (Exception e) {
            LOGGER.log(Level.ALL, e.getMessage());
        } finally {
            return insertStatement;
        }
    }
}
