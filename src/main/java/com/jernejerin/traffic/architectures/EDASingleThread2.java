package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.entities.Cell;
import com.jernejerin.traffic.entities.Route;
import com.jernejerin.traffic.helper.PollingDriver;
import com.jernejerin.traffic.helper.TaxiStream;
import com.jernejerin.traffic.helper.TripOperations;
import org.apache.commons.cli.*;
import reactor.Environment;
import reactor.fn.tuple.Tuple;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Streams;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * <p>
 * An example of single threaded event driven architecture - EDA.
 * </p>
 *
 * @author Jernej Jerin
 */
public class EDASingleThread2 {
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

    private static File fileQuery1;
    private static File fileQuery2;

    private final static Logger LOGGER = Logger.getLogger(EDASingleThread2.class.getName());

    // initial table size is 1e5
//    private static Map<Route, Route> routes = new LinkedHashMap<>(100000);

    private static Map<Cell, Cell> cells = new LinkedHashMap<>(100000);

    // current top 10 sorted
    private static List<Route> top10PreviousQuery1 = new LinkedList<>();
    private static LinkedList<Cell> top10PreviousQuery2 = new LinkedList<>();

    private static Deque<Route> routes = new ArrayDeque<>();

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting single threaded EDA solution from thread = " + Thread.currentThread());

        // set host and port from command line options
        setOptionsCmd(args);

        // output file
        /* The default output file path and name. */
        String fileNamePathQuery1 = "./EDA_experimental_query1_frequent_routes.txt";
        fileQuery1 = new File(fileNamePathQuery1);
        String fileNamePathQuery2 = "./EDA_experimental_query2_profitable_cells.txt";
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
        taxiStream.getTrips()/*.log("broadcaster")*/
            .map(t -> {
                // create a tuple of string trip and current time for computing delay
                // As this is our entry point it is appropriate to start the time here,
                // before any parsing is being done. This also in record with the Grand
                // challenge recommendation
                //                LOGGER.log(Level.INFO, "Distributing for ticket = " +
//                        t + " from thread = " + Thread.currentThread());
                return Tuple.of(t, System.currentTimeMillis());
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
                                        while (routes.peek() != null && routes.peek().getDropOffDatetime().isBefore(
                                                t.getDropOffDatetime().minusMinutes(30))) {
                                            Route route = routes.poll();

                                            // recompute top10 only if the polled route is in the current top 10
                                            if (top10PreviousQuery1.contains(route)) {
                                                List<Route> bestRoutes = bestRoutes();

                                                // if there is change in top 10, write it
                                                writeTop10ChangeQuery1(bestRoutes, route.getPickupDatetime().plusMinutes(30),
                                                        route.getDropOffDatetime().plusMinutes(30),
                                                        t.getTimestampReceived());
                                            }
                                        }

                                        routes.add(t.getRoute());
                                        List<Route> bestRoutes = bestRoutes();
                                        return Tuple.of(bestRoutes, t.getPickupDatetime(), t.getDropOffDatetime(),
                                                t.getTimestampReceived());
                                    })
                                    .consume(ct -> writeTop10ChangeQuery1(ct.getT1(), ct.getT2(), ct.getT3(), ct.getT4()));
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
     * Computes top 10 best routes sorted by frequency and last freshest event.
     *
     * @return a list of 10 best routes
     */
    private static List<Route> bestRoutes() {
        // a priority queue for top 10 routes. Orders by natural number.
        BoundedPriorityQueue<Route> top10 = new BoundedPriorityQueue<>(Comparator.<Route>naturalOrder(), 10);

        Map<Route, Integer> routesCounted = routes.stream()
                // group by the same routes
                .collect(Collectors.groupingBy(r -> r))
                // we get a list of routes
                .values().stream()
                .collect(Collectors.toMap(
                        // select for key the route, which was updated last. For the value set the list size.
                        lst -> lst.stream().max(Comparator.comparing(Route::getLastUpdated)).get(),
                        List::size
                ));
        routesCounted.forEach((r, c) -> {
            r.setDropOffSize(c.intValue());
            top10.offer(r);
        });


        List<Route> top10SortedNew = new LinkedList<>(top10);
        // sort routes
        top10SortedNew.sort(Comparator.<Route>reverseOrder());

        return  top10SortedNew;
    }

    /**
     * Outputs a log to a file when top 10 routes is changed.
     *
     * @param top10 the new top 10 routes
     * @param pickupDateTime the pickup date time of the event, that changed the top 10 routes
     * @param dropOffDateTime the drop off date time of the event that change the top 10 routes
     * @param delay the delay of between reading the event and writing the event
     */
    private static void writeTop10ChangeQuery1(List<Route> top10, LocalDateTime pickupDateTime,
                                         LocalDateTime dropOffDateTime, Long delay) {
        // write to file stream for query 1 if the top 10 queue was changed
        if (!top10.equals(top10PreviousQuery1)) {
            top10PreviousQuery1 = top10;

            // build content string for output
            String content = pickupDateTime.toString() + ", " + dropOffDateTime.toString() + ", ";

            // iterate over all the most frequent routes
            for (Route route : top10) {
                content += route.getStartCell().getEast() + "." + route.getStartCell().getSouth() +
                        ", " + route.getEndCell().getEast() + "." + route.getEndCell().getSouth() + " (" + route.getDropOffSize() + "), ";
            }

            // add a delay
            content += System.currentTimeMillis() - delay + "\n";

            try (FileOutputStream fop = new FileOutputStream(fileQuery1, true)) {
                // write to file
                fop.write(content.getBytes());
                fop.flush();
                fop.close();
            } catch (IOException ex) {
                LOGGER.log(Level.SEVERE, ex.getMessage());
            }
        }
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