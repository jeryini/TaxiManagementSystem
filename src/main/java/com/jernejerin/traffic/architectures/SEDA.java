package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.entities.Cell;
import com.jernejerin.traffic.entities.Route;
import com.jernejerin.traffic.entities.Taxi;
import com.jernejerin.traffic.entities.Trip;
import com.jernejerin.traffic.helper.PollingDriver;
import com.jernejerin.traffic.helper.TaxiStream;
import com.jernejerin.traffic.helper.TripOperations;
import org.apache.commons.cli.*;
import reactor.Environment;
import reactor.core.DispatcherSupplier;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Streams;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.ZoneOffset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * An example of single threaded event driven architecture - EDA.
 * </p>
 *
 * @author Jernej Jerin
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
    private static String schemaDB = "taxi_trip_management";

    /** The default input file name. */
    private static String fileName = "trips_20_days.csv";

    private static File fileQuery1;
    private static File fileQuery2;

    private final static Logger LOGGER = Logger.getLogger(SEDA.class.getName());



    // initial table size is 1e5
    private static Map<Route, Route> routes = new LinkedHashMap<>(100000);

    private static Map<Cell, Cell> cells = new LinkedHashMap<>(100000);

    // current top 10 sorted
    private static LinkedList<Route> top10PreviousQuery1 = new LinkedList<>();
    private static LinkedList<Cell> top10PreviousQuery2 = new LinkedList<>();

    /** The default number of threads for stage 1. */
    private static int stage1T = 20;

    /** The default number of threads for stage 2. */
    private static int stage2T = 20;

    public static void main(String[] args) throws Exception {
        LOGGER.log(Level.INFO, "Starting single threaded EDA solution from thread = " + Thread.currentThread());

        // set host and port from command line options
        setOptionsCmd(args);

        // suppliers for stages
        DispatcherSupplier supplierStage1 = Environment.newCachedDispatchers(stage1T, "stage1");
        DispatcherSupplier supplierStage2 = Environment.newCachedDispatchers(stage2T, "stage2");

        // output file
        /* The default output file path and name. */
        String fileNamePathQuery1 = "./EDA_single_thread_query1_frequent_routes.txt";
        fileQuery1 = new File(fileNamePathQuery1);
        String fileNamePathQuery2 = "./EDA_single_thread_query2_profitable_cells.txt";
        fileQuery2 = new File(fileNamePathQuery2);

        // environment initialization
        Environment env = Environment.initializeIfEmpty().assignErrorJournal();

        // create a new streaming object
        TaxiStream taxiStream = new TaxiStream("/com/jernejerin/" + fileName);

        // TCP server
        TcpServer<String, String> server = NetStreams.tcpServer(
                spec -> spec
                        .env(env)
                        .listen(hostTCP, portTCP)
                        .dispatcher(Environment.cachedDispatcher())
                        .codec(StandardCodecs.STRING_CODEC)
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
        taxiStream.getTrips()
            // dispatcher for funneling/joining result back to single thread
            .dispatchOn(Environment.sharedDispatcher())
            .map(t -> {
                // create a tuple of string trip and current time for computing delay
                // As this is our entry point it is appropriate to start the time here,
                // before any parsing is being done. This also in record with the Grand
                // challenge recommendation
                //                LOGGER.log(Level.INFO, "Distributing for ticket = " +
//                        t + " from thread = " + Thread.currentThread());
                return Tuple.of(t, System.currentTimeMillis());
            })
            // parallelize stream tasks to separate streams for stage 1 - PARSING AND SAVING DATA
            .partition(stage1T)
            // we receive streams grouped by accordingly to the positive modulo of the
            // current hashcode with respect to the number of buckets specified
            .flatMap(stream -> stream
                // dispatch on separate stream
                .dispatchOn(supplierStage1.get())
                // stage 1 is for validating ticket structure and saving it into the DB
                .map(t -> TripOperations.parseValidateTrip(t.getT1(), t.getT2()))
                // group by trip validation
                .groupBy(t -> t != null)
                .map(tripValidStream -> {
                    // if trip is valid continue with operations on the trip
                    // The insert trip operation falls under the STAGE 1.
                    if (tripValidStream.key()) {
                        return tripValidStream
                                .map(t -> {
                                    // save ticket
//                            TripOperations.insertTrip(t);
                                    return t;
                                });
                    } else {
                        tripValidStream.consume(trip ->
                            LOGGER.log(Level.WARNING, "Invalid trip passed in!")
                        );
                    }
                    return Streams.<Object>empty();
                })
            )
            .dispatchOn(Environment.sharedDispatcher())
            .consume(t -> {
                t.consume();
            });

                    // partition stream for stage 2
//                        .partition(stage2T)
//                        .flatMap(stream -> stream
//                                        // dispatcher on supplier for stage 2
//                                        .dispatchOn(supplierStage2.get())
//                                                // TODO (Jernej Jerin): get current statistics for vehicle
//                                        .map(bt -> bt)
//                                                // TODO (Jernej Jerin): compute new statistics. We would like to
//                                                // TODO (Jernej Jerin): have some heavy CPU operation here to simulate
//                                                // TODO (Jernej Jerin): CPU intensive work.
//                                        .map(bt -> bt)
//                        )
//                        .dispatchOn(Environment.sharedDispatcher())
//                        .consume();
//
        // read the stream from file: for local testing
        taxiStream.readStream();

        // run the server forever
        // TODO(Jernej Jerin): Is there a better way to do this?
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * <p>
     * Set options from passed command line arguments. The following
     * options are set:
     *  - host TCP
     *  - port TCP
     *  - host DB
     *  - port DB
     *  - user DB
     *  - pass DB
     *  - schema DB
     *  - file name
     * It also prints the display help if user passes in help option.
     * </p>
     *
     * @param args an array of command line arguments
     */
    public static void setOptionsCmd(String[] args) {
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
        options.addOption("fileName", true, "the name of the file that holds the data");

        // parser for command line arguments
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        // display help
        if (cmd.hasOption("help")) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("java -jar EDA", options);
            System.exit(-1);
        }

        // set values
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
        if (cmd.getOptionValue("fileName") != null)
            fileName = cmd.getOptionValue("fileName");
    }
}
