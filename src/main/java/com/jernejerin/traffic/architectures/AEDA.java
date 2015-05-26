package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.entities.*;
import com.jernejerin.traffic.helper.TaxiStream;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.fn.tuple.Tuple;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * <p>
 * An example of a Actor based event driven architecture - AEDA.
 *
 * @author Jernej Jerin
 */
public class AEDA extends Architecture {
    private final static Logger LOGGER = Logger.getLogger(AEDA.class.getName());
    private static int id = 0;

    public AEDA(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting single threaded AEDA 3 solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder().fileNameQuery1Output("output/" +
                AEDA.class.getSimpleName() + "_query1.txt").fileNameQuery2Output("output/" +
                AEDA.class.getSimpleName() + "_query2.txt");

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the AEDA solution using the builder
        Architecture eda = new AEDA(builder);

        // run the solution
        eda.run();
    }

    public long run() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // create a taxi service
        this.taxiStream = new TaxiStream("/com/jernejerin/" + this.fileNameInput);

        // time windows
        final ArrayDeque<Trip> trips = new ArrayDeque<>();
        final LinkedHashMap<Route, RouteCount> routesCount = new LinkedHashMap<>(100000);

        CountDownLatch completeSignal = new CountDownLatch(2);


        taxiStream.getTrips()
                .map(t -> {
                    // create a tuple of string trip and current time for computing delay
                    // As this is our entry point it is appropriate to start the time here,
                    // before any parsing is being done. This also in record with the Grand
                    // challenge recommendation
                    return Tuple.of(t, System.currentTimeMillis(), id++);
                })
                .observeComplete(v -> {
                    // send complete events to each query
                    taxiStream.query1.onComplete();
                    taxiStream.query2.onComplete();
                })
                        // parsing and validating trip structure
                .map(t -> TripOperations.parseValidateTrip(t.getT1(), t.getT2(), t.getT3()))
                        // group by trip validation
                .groupBy(t -> t != null)
                .consume(tripValidStream -> {
                    // if trip is valid continue with operations on the trip
                    if (tripValidStream.key()) {
                        tripValidStream
                                .groupBy(t -> t.getRoute250() != null)
                                .consume(routeValidStream -> {
                                    if (routeValidStream.key()) {
                                        routeValidStream
                                                .consume(t -> {
                                                    taxiStream.query1.onNext(t);
                                                    taxiStream.query2.onNext(t);
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
        // query 1: Frequent routes
        taxiStream.query1
                .map(t -> {
                    // trips leaving the window
                    while (trips.peek() != null && trips.peek().getDropOffTimestamp() < t.getDropOffTimestamp()
                            - 30 * 60 * 1000) {
                        // remove it from queue
                        Trip trip = trips.poll();


                    }

                    // add to window
                    trips.add(t);


                    return null;
                })
                .observeComplete(v -> {
                    completeSignal.countDown();
                })
                .consume(ct -> {

                });

        // query 2: Frequent routes
        taxiStream.query2
                .observeComplete(v -> {
                    completeSignal.countDown();
                })
                .consume();

        // read the stream from file: for local testing
        taxiStream.readStream();

        // wait for onComplete event
        completeSignal.await();
        id = 0;
        return System.currentTimeMillis() - startTime;
    }
}
