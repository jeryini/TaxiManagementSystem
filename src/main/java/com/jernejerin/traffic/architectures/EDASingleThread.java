package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.entities.Route;
import com.jernejerin.traffic.entities.Trip;
import com.jernejerin.traffic.helper.PollingDriver;
import com.jernejerin.traffic.helper.TaxiStream;
import com.jernejerin.traffic.helper.TripOperations;
import org.apache.commons.cli.*;
import reactor.Environment;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Streams;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * An example of single threaded event driven architecture - EDA.
 * </p>
 *
 * @author Jernej Jerin
 */
public class EDASingleThread {
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
    private static String fileName = "trips_example.csv";

    /** The default output file path and name. */
    private static String  fileNamePathQuery1 = "./query1_frequent_routes.txt";

    private static File fileQuery1;

    private final static Logger LOGGER = Logger.getLogger(EDASingleThread.class.getName());

    // initial table size is 1e5
    private static Map<Route, LinkedBlockingQueue<Long>> routeMap = new HashMap<>(100000);

    // a priority queue for top 10 routes. Orders by natural number.
    private static BoundedPriorityQueue<Route> top10 = new BoundedPriorityQueue<>(Comparator.<Route>naturalOrder(), 10);

    public static void main(String[] args) throws InterruptedException {



        LOGGER.log(Level.INFO, "Starting single threaded EDA solution from thread = " + Thread.currentThread());

        // set host and port from command line options
        setOptionsCmd(args);

        // output file
        fileQuery1 = new File(fileNamePathQuery1);

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
            ch.log("conn").consume(trip -> {
                LOGGER.log(Level.INFO, "TCP server receiving trip " +
                    trip + " from thread = " + Thread.currentThread());
                // dispatch event to a broadcaster pipeline, which uses the same number of threads
                // as there are cores
                taxiStream.getTrips().onNext(trip);
                LOGGER.log(Level.INFO, "TCP server send ticket to streaming pipeline for ticket = " +
                    trip + " from thread = " + Thread.currentThread());
            });
            return Streams.never();
        }).await();

        // Then we set up and register the PoolingDriver.
        LOGGER.log(Level.INFO, "Registering pooling driver from thread = " + Thread.currentThread());
        try {
            PollingDriver.setupDriver("jdbc:mysql://" + hostDB + ":" + portDB + "/" + schemaDB, userDB, passDB);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // processing through streams, where number of threads is the same as number of cores
        taxiStream.getTrips().log("broadcaster")
            .map(t -> {
                // create a tuple of string trip and current time for computing delay
                // As this is our entry point it is appropriate to start the time here,
                // before any parsing is being done. This also in record with the Grand
                // challenge recommendation
                Tuple2<String, Long> tripTime = Tuple.of(t, System.currentTimeMillis());
                LOGGER.log(Level.INFO, "Distributing for ticket = " +
                        t + " from thread = " + Thread.currentThread());
                return tripTime;
            })
            // parsing and validating trip structure
            .map(t -> TripOperations.parseValidateTrip(t.getT1(), t.getT2()))
            // group by trip validation
            .groupBy(t -> t != null)
            .consume(tripValidStream -> {
                // if trip is valid continue with operations on the trip
                if (tripValidStream.key()) {
                    tripValidStream
                        .map(t -> {
                            // save ticket
//                            TripOperations.insertTrip(t);
                            return t;
                        })
                        // group by if route for the trip is valid, as
                        // we need route in follow up operations
                        .groupBy(t -> t.getRoute() != null)
                        .consume(routeValidStream -> {
                            if (routeValidStream.key()) {
                                routeValidStream
                                    // query 1: Frequent routes
                                    .map(t -> {
                                        // get value in map or default value which is empty queue
                                        LinkedBlockingQueue<Long> dropOff = routeMap.getOrDefault(t.getRoute(), t.getRoute().getDropOff());

                                        // set the latest timestamp
                                        dropOff.add(t.getDropOffDatetime().toEpochSecond(ZoneOffset.UTC) * 1000);
                                        t.getRoute().setDropOff(dropOff);
                                        routeMap.put(t.getRoute(), dropOff);

                                        // try to add it to top 10 of the frequent routes.
                                        // These are sorted by drop off size and latest drop off timestamps
                                        boolean changed = false;

                                        // TODO (Jernej Jerin): Better to maintain separate set for inserting routes
                                        // TODO (Jernej Jerin): into the top10 list
                                        top10.remove(t.getRoute());
                                        changed = top10.offer(t.getRoute());

                                        List<Route> routes = new LinkedList<Route>(top10);

                                        return Tuple.of(changed, t.getPickupDatetime(), t.getDropOffDatetime(),
                                                routes, t.getTimestampReceived());
                                    })
                                    // TODO (Jernej Jerin): Add CPU intensive task for query 2: Profitable areas
                                    //            .map(bt -> {
                                    //                return bt;
                                    //            })
                                    .consume(ct -> {
                                        // write to file stream if the top 10 queue was changed
                                        if (ct.getT1()) {
                                            ct.getT4().sort(Comparator.<Route>reverseOrder());

                                            // build content string for output
                                            String content = ct.getT2().toString() + ", " + ct.getT3().toString() + ", ";

                                            // iterate over all the most frequent routes
                                            for (Route route : ct.getT4()) {
                                                content += route.getStartCell().getEast() + "." + route.getStartCell().getSouth() +
                                                        ", " + route.getEndCell().getEast() + "." + route.getEndCell().getSouth() + ", ";
                                            }

                                            // add a delay
                                            content += System.currentTimeMillis() - ct.getT5() + "\n";

                                            try (FileOutputStream fop = new FileOutputStream(fileQuery1, true)) {
                                                // write to file
                                                fop.write(content.getBytes());
                                                fop.flush();
                                                fop.close();
                                            } catch (IOException ex) {
                                                LOGGER.log(Level.SEVERE, ex.getMessage());
                                            }
                                        }
                                    });
                            } else {
                                routeValidStream.consume(trip ->
                                                LOGGER.log(Level.WARNING, "Route is not valid for trip!")
                                );
                            }
                        });
                } else {
                    tripValidStream.consume(trip ->
                                    LOGGER.log(Level.WARNING, "Invalid trip passed in!")
                    );
                }
            });

        // read the stream from file: for local testing
        taxiStream.readStream();

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
