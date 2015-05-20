package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.entities.Cell;
import com.jernejerin.traffic.entities.Route;
import com.jernejerin.traffic.entities.RouteCount;
import com.jernejerin.traffic.entities.Taxi;
import com.jernejerin.traffic.helper.TaxiStream;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.rx.Streams;

import java.io.FileOutputStream;
import java.io.IOException;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * <p>
 * An example of a event driven architecture - EDA.
 *
 * @author Jernej Jerin
 */
public class EDA extends Architecture {
    private final static Logger LOGGER = Logger.getLogger(EDASingleThread3.class.getName());

    public EDA(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting single threaded EDA 3 solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder();

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the EDA solution using the builder
        EDASingleThread3 eda3 = new EDASingleThread3(builder);

        // run the solution
        eda3.run();
    }

    public long run() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // create a taxi service
        this.taxiStream = new TaxiStream("/com/jernejerin/" + this.fileNameInput);

        // current top 10 sorted
        final List<RouteCount> top10Routes = new LinkedList<>();
        CountDownLatch completeSignal = new CountDownLatch(1);
        Queue<Route> routes = new ArrayDeque<>();

        taxiStream.getTrips()
                .map(t -> {
                    // create a tuple of string trip and current time for computing delay
                    // As this is our entry point it is appropriate to start the time here,
                    // before any parsing is being done. This also in record with the Grand
                    // challenge recommendation
                    return Tuple.of(t, System.currentTimeMillis());
                })
                .observeComplete(v -> completeSignal.countDown())
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
                                                .consume(t -> {
                                                    taxiStream.query1.onNext(t);
                                                    taxiStream.query2.onNext(t);
                                                });
                                    } else {
                                        routeValidStream.consume(//trip ->
//                                                        LOGGER.log(Level.WARNING, "Route is not valid for trip!")
                                        );
                                    }
                                });
                    } else {
                        tripValidStream.consume(//trip ->
//                                        LOGGER.log(Level.WARNING, "Invalid trip passed in!")
                        );
                    }
                });
        taxiStream.query1
                // query 1: Frequent routes
                .map(t -> {
                    while (routes.peek() != null && routes.peek().getDropOffDatetime().isBefore(
                            t.getDropOffDatetime().minusMinutes(30))) {
                        Route route = routes.poll();
                        // recompute top10 only if the polled route is in the current top 10
                        if (top10Routes.contains(route)) {
                            List<RouteCount> bestRoutes = bestRoutes(routes);
                            if (!top10Routes.equals(bestRoutes)) {
                                top10Routes.clear();
                                top10Routes.addAll(bestRoutes);
                                // if there is change in top 10, write it
                                writeTop10ChangeQuery1(bestRoutes, route.getPickupDatetime().plusMinutes(30),
                                        route.getDropOffDatetime().plusMinutes(30),
                                        t.getTimestampReceived());
                            }
                        }
                    }

                    // add to window
                    routes.add(t.getRoute());
                    List<RouteCount> bestRoutes = bestRoutes(routes);
                    return Tuple.of(bestRoutes, t.getPickupDatetime(), t.getDropOffDatetime(),
                            t.getTimestampReceived());
                })
                .consume(ct -> {
                    if (!top10Routes.equals(ct.getT1())) {
                        top10Routes.clear();
                        top10Routes.addAll(ct.getT1());
                        writeTop10ChangeQuery1(ct.getT1(), ct.getT2(), ct.getT3(), ct.getT4());
                    }
                });

        taxiStream.query2
                .consume();

        // read the stream from file: for local testing
        taxiStream.readStream();

        // wait for onComplete event
        completeSignal.await();
        return System.currentTimeMillis() - startTime;
    }


    /**
     * Computes top 10 best routes sorted by frequency and last freshest event.
     *
     * @param routes a window of routes in past 30 minutes
     * @return a list of 10 best routes
     */
    private static List<RouteCount> bestRoutes(Queue<Route> routes) {
        // a priority queue for top 10 routes. Orders by natural number.
        BoundedPriorityQueue<RouteCount> top10 = new BoundedPriorityQueue<>(Comparator.<RouteCount>naturalOrder(), 10);

        Map<Route, RouteCount> routesCounted = routes.stream()
                .collect(Collectors.groupingBy(route -> route,
                                Collectors.collectingAndThen(
                                        Collectors.mapping(RouteCount::fromRoute, Collectors.reducing(RouteCount::combine)),
                                        Optional::get
                                )
                        )
                );

        routesCounted.forEach((r, rc) -> {
            top10.offer(rc);
        });


        List<RouteCount> top10SortedNew = new LinkedList<>(top10);
        // sort routes
        top10SortedNew.sort(Comparator.<RouteCount>reverseOrder());

        return top10SortedNew;
    }

    /**
     * Computes top 10 best routes sorted by frequency and last freshest event. Compare to upper method
     * this was proven to be of worse performance.
     *
     * @param routes a window of routes in past 30 minutes
     * @return a list of 10 best routes
     */
    private List<RouteCount> bestRoutesPerformanceWorse(Queue<Route> routes) {
        // a priority queue for top 10 routes. Orders by natural number.
        BoundedPriorityQueue<RouteCount> top10 = new BoundedPriorityQueue<>(Comparator.<RouteCount>naturalOrder(), 10);

        // we are performing only stateless intermediate operations
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
            top10.offer(new RouteCount(r, c));
        });

        // sort routes
        List<RouteCount> top10SortedNew = new LinkedList<>(top10);
        top10SortedNew.sort(Comparator.<RouteCount>reverseOrder());

        return  top10SortedNew;
    }
}
