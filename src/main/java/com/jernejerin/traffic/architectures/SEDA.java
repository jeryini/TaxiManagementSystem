package com.jernejerin.traffic.architectures;

import com.jernejerin.traffic.entities.Trip;
import org.apache.commons.cli.*;
import reactor.Environment;
import reactor.core.DispatcherSupplier;
import reactor.fn.Consumer;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.ChannelStream;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * Created by Jernej Jerin on 16.4.2015.
 */
public class SEDA {

    /** The default hostname of the TCP server. */
    private static String hostTCP = "localhost";

    /** The default port of the TCP server. */
    private static int portTCP = 30000;

    /** The default host of the DB server. */
    private static String hostDB = "localhost";

    /** The default port of the DB server. */
    private static int portDB = 3307;

    /** The default user of the DB server. */
    private static String userDB = "root";

    /** The default password of the DB server. */
    private static String passDB = "password";

    /** The default schema name to use. */
    private static String schemaDB = "traffic_management";

    /** The default number of threads for stage 1. */
    private static int stage1T = 20;

    /** The default number of threads for stage 2. */
    private static int stage2T = 20;

    private final static Logger LOGGER = Logger.getLogger(EDA.class.getName());

    public static void main(String[] args) throws Exception {
        // options for specifying command line options
        Options options = new Options();

        // add arguments
        options.addOption("help", false, "help for usage");
        options.addOption("hostTCP", true, "the hostname of the TCP server");
        options.addOption("portTCP", true, "the port of the TCP server");
        options.addOption("hostDB", true, "the hostname of the DB server");
        options.addOption("portDB", true, "the port of the DB server");
        options.addOption("userDB", true, "the username for the DB server");
        options.addOption("passDB", true, "the password for the DB server");
        options.addOption("schemaDB", true, "the schema name of the DB to use");
        options.addOption("stage1T", true, "number of threads for stage 1");
        options.addOption("stage2T", true, "number of threads for stage 2");

        // parser for command line arguments
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("java -jar SEDA", options);
            System.exit(-1);
        }
        if (cmd.getOptionValue("hostTCP") != null)
            hostTCP = cmd.getOptionValue("hostTCP");
        if (cmd.getOptionValue("portTCP") != null)
            portTCP = Integer.parseInt(cmd.getOptionValue("portTCP"));
        if (cmd.getOptionValue("hostDB") != null)
            hostDB = cmd.getOptionValue("hostDB");
        if (cmd.getOptionValue("portDB") != null)
            portDB = Integer.parseInt(cmd.getOptionValue("portDB"));
        if (cmd.getOptionValue("userDB") != null)
            userDB = cmd.getOptionValue("userDB");
        if (cmd.getOptionValue("passDB") != null)
            passDB = cmd.getOptionValue("passDB");
        if (cmd.getOptionValue("schemaDB") != null)
            schemaDB = cmd.getOptionValue("schemaDB");
        if (cmd.getOptionValue("stage1T") != null)
            stage1T = Integer.parseInt(cmd.getOptionValue("stage1T"));
        if (cmd.getOptionValue("stage2T") != null)
            stage2T = Integer.parseInt(cmd.getOptionValue("stage2T"));

        // environment initialization
        Environment.initializeIfEmpty().assignErrorJournal();

        // event driven broadcaster
        Broadcaster<Trip> stageBroadcaster = Broadcaster.create(Environment.get());

        DispatcherSupplier supplierStage1 = Environment.newCachedDispatchers(stage1T, "stage1");
        DispatcherSupplier supplierStage2 = Environment.newCachedDispatchers(stage2T, "stage2");

        // codec for Trip class
        JsonCodec<Trip, Trip> codec = new JsonCodec<Trip, Trip>(Trip.class);

        // get prepared statement for inserting ticket into database
        PreparedStatement insertTicket = getInsertPreparedStatement();
        if (insertTicket == null)
            throw new Exception();

        // TCP server
        TcpServer<Trip, Trip> server = NetStreams.tcpServer(
                spec -> spec
                        .listen(portTCP)
                        .codec(codec)
                        .dispatcher(Environment.cachedDispatcher())
        );

        // consumer for TCP server
        server.start(ch -> {
            ch.consume(trip -> {
                LOGGER.log(Level.INFO, "TCP server receiving trip " +
                        trip + " from thread = " + Thread.currentThread());
                // dispatch event to a broadcaster pipeline, which uses the same number of threads
                // as there are cores
//                trips.onNext(trip);
                LOGGER.log(Level.INFO, "TCP server send ticket to streaming pipeline for ticket = " +
                        trip + " from thread = " + Thread.currentThread());
            });
            return Streams.never();
        });

        // processing through stages of streams, where each stage has a separate thread pool
        // for processing streams. We basically fork the stream in each stage to number of streams,
        // which equals the number of threads in pool
        stageBroadcaster
                // dispatcher for funneling/joining result back to single thread
                .dispatchOn(Environment.sharedDispatcher())
                // parallelize stream tasks to separate streams for stage 1
                .partition(stage1T)
                // we receive streams grouped by accordingly to the positive modulo of the
                // current hashcode with respect to the number of buckets specified
                .flatMap(stream -> stream
                    // dispatch on separate stream
                    .dispatchOn(supplierStage1.get())
                    // stage 1 is for validating ticket structure and saving it into the DB
                    // TODO (Jernej Jerin): Maybe we should create separate classes for ticket validation
                    // TODO (Jernej Jerin): and ticket saving for code reuse in EDA and ASEDA architecture.
                    // TODO (Jernej Jerin): validate ticket structure
                    .map(bt -> bt)
                    // insert ticket to DB using prepared statement
                    .map(bt -> {
                        try {
                            // TODO (Jernej Jerin): Find out why we get duplicate values.
                            System.out.printf("Insert ticket %s into DB from thread %s", bt.getId(), Thread.currentThread());
//                            insertTicket.setInt(1, bt.getId());
//                            insertTicket.setLong(2, bt.getStartTime());
//                            insertTicket.setLong(3, bt.getLastUpdated());
//                            insertTicket.setDouble(4, bt.getSpeed());
//                            insertTicket.setInt(5, bt.getCurrentLaneId());
//                            insertTicket.setInt(6, bt.getPreviousSectionId());
//                            insertTicket.setInt(7, bt.getNextSectionId());
//                            insertTicket.setDouble(8, bt.getSectionPositon());
//                            insertTicket.setInt(9, bt.getDestinationId());
//                            insertTicket.setInt(10, bt.getVehicleId());

                            insertTicket.execute();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                        // pass forward base ticket
                        return bt;
                    })
                )
                // partition stream for stage 2
                .partition(stage2T)
                .flatMap(stream -> stream
                    // dispatcher on supplier for stage 2
                    .dispatchOn(supplierStage2.get())
                    // TODO (Jernej Jerin): get current statistics for vehicle
                    .map(bt -> bt)
                    // TODO (Jernej Jerin): compute new statistics. We would like to
                    // TODO (Jernej Jerin): have some heavy CPU operation here to simulate
                    // TODO (Jernej Jerin): CPU intensive work.
                    .map(bt -> bt)
                )
                .dispatchOn(Environment.sharedDispatcher())
                .consume();

        server.shutdown().await();

        // run the server forever
        // TODO(Jernej Jerin): Is there a better way to do this?
        Thread.sleep(Long.MAX_VALUE);
    }

    private static PreparedStatement getInsertPreparedStatement() {
        PreparedStatement insertStatement = null;
        try {
            // create a mysql database connection
            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://" + hostDB + ":" + portDB + "/" + schemaDB;

            // MySQL connection settings
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, userDB, passDB);

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
