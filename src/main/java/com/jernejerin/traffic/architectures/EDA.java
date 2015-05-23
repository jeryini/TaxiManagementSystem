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
 * An example of a single threaded event driven architecture - EDA.
 * This solution consists of LinkedHashMap.
 *
 * @author Jernej Jerin
 */
public class EDA extends Architecture {
    private final static Logger LOGGER = Logger.getLogger(EDAPrimer.class.getName());
    private static int id = 0;

    public EDA(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting single threaded EDA 3 solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder().fileNameQuery1Output("output/" +
                EDA.class.getSimpleName() + "_query1.txt").fileNameQuery2Output("output/" +
                EDA.class.getSimpleName() + "_query2.txt");

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the EDA solution using the builder
        Architecture eda = new EDA(builder);

        // run the solution
        eda.run();
    }

    public long run() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // create a taxi service
        this.taxiStream = new TaxiStream("/com/jernejerin/" + this.fileNameInput);

        // current top 100 sorted
        final LinkedList<RouteCount> top100Routes = new LinkedList<>();

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

                        // update the route count for the route of the trip, leaving the window
                        RouteCount routeCount = routesCount.get(trip.getRoute500());
                        routeCount.setCount(routeCount.getCount() - 1);

                        // if count is 0 then remove the route count to avoid unnecessary iteration
                        if (routeCount.getCount() == 0)
                            routesCount.remove(routeCount);
                        else
                            routesCount.put(trip.getRoute500(), routeCount);

                        // if route is in top 100, then resort the top 100
                        // if it is not in top 100, then we do not have to do
                        // anything because counting down the route count
                        // will not change the top 100
                        int i = top100Routes.indexOf(routeCount);
                        if (i != -1) {
                            List<RouteCount> top10 = null;

                            // save current top 10 for future comparison only if
                            // the route of the removed trip lies in top 10
                            if (i <= 9) {
                                top10 = new LinkedList<>(top100Routes.subList(0, top100Routes.size() < 10 ?
                                        top100Routes.size() : 10));
                            }

                            // flip elements from i-th to the last one
                            // until the current route is smaller
                            for (int j = i, k = i + 1; k < top100Routes.size(); j++, k++) {
                                // route count at current position is smaller, then switch places
                                if (top100Routes.get(j).compareTo(top100Routes.get(k)) < 0) {
                                    Collections.swap(top100Routes, j, k);
                                }
                            }

                            // if the element falls into the last place in top 100, then we need to
                            // resort all route count
                            if (top100Routes.get(top100Routes.size() - 1).equals(routeCount)) {
                                // a priority queue for top 10 routes. Orders by natural number.
                                BoundedPriorityQueue<RouteCount> newTop100Routes = new BoundedPriorityQueue<>
                                        (Comparator.<RouteCount>naturalOrder(), 100);

                                // try to add each route count to the top 100 list. This way we get sorted top 100 with
                                // time complexity n * log(100) + 100 * log(100) vs. n * log(n)
                                routesCount.forEach((r, rc) -> newTop100Routes.offer(rc));

                                // copy to the top 100 routes and resort (100 * log(100))
                                Collections.copy(top100Routes, new LinkedList<>(newTop100Routes));
                                Collections.sort(top100Routes, Comparator.<RouteCount>reverseOrder());
                            }

                            // check if top 10 has changed
                            if (top10 != null && !top10.equals(top100Routes.subList(0, top100Routes.size() < 10 ?
                                    top100Routes.size() : 10))) {
                                writeTop10ChangeQuery1(top100Routes.subList(0, top100Routes.size() < 10 ? top100Routes.size() : 10),
                                        trip.getPickupDatetime().plusMinutes(30),
                                        trip.getDropOffDatetime().plusMinutes(30),
                                        t.getTimestampReceived(), trip);
                            }
                        }
                    }

                    // add to window
                    trips.add(t);

                    // update the route count and trip id for the route of the incoming trip
                    // if the rout count does not yet exist, create a new one
                    RouteCount routeCount = routesCount.getOrDefault(t.getRoute500(),
                            new RouteCount(t.getRoute500(), t.getId(), 0));
                    routeCount.setCount(routeCount.getCount() + 1);
                    routeCount.setId(t.getId());
                    routesCount.put(t.getRoute500(), routeCount);

                    // store current top 10
                    LinkedList<RouteCount> top10 = new LinkedList<>(top100Routes.subList(0, top100Routes.size() < 10 ?
                            top100Routes.size() : 10));

                    int i = top100Routes.indexOf(routeCount);
                    if (i != -1) {
                        top100Routes.set(i, routeCount);
                        // flip elements from i-th to the first one
                        // until the current route is larger
                        for (int j = i - 1, k = i; j >= 0; j--, k--) {
                            // route count at current position is smaller, then switch places
                            if (top100Routes.get(j).compareTo(top100Routes.get(k)) < 0) {
                                Collections.swap(top100Routes, j, k);
                            }
                        }
                    }
                    else {
                        // check top 100 size
                        if (top100Routes.size() < 100) {
                            top100Routes.addLast(routeCount);
                        }
                        // compare it with the last element in top 100
                        else if (top100Routes.getLast().compareTo(routeCount) < 0) {
                            top100Routes.set(99, routeCount);
                        }

                        for (int j = top100Routes.size() - 2, k = top100Routes.size() - 1; j >= 0; j--, k--) {
                            // route count at current position is smaller, then switch places
                            if (top100Routes.get(j).compareTo(top100Routes.get(k)) < 0) {
                                Collections.swap(top100Routes, j, k);
                            }
                        }
                    }
                    return Tuple.of(top10, t.getPickupDatetime(), t.getDropOffDatetime(),
                            t.getTimestampReceived(), t);

                })
                .observeComplete(v -> {
                    completeSignal.countDown();
                })
                .consume(ct -> {
                    if (!ct.getT1().equals(top100Routes.subList(0, top100Routes.size() < 10 ?
                            top100Routes.size() : 10))) {
                        writeTop10ChangeQuery1(top100Routes.subList(0, top100Routes.size() < 10 ?
                                        top100Routes.size() : 10), ct.getT2(), ct.getT3(), ct.getT4(),
                                ct.getT5());
                    }
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
