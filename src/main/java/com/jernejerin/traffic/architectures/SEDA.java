package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.client.TaxiStream;
import com.jernejerin.traffic.entities.Cell;
import com.jernejerin.traffic.entities.Route;
import com.jernejerin.traffic.entities.RouteCount;
import com.jernejerin.traffic.entities.Trip;
import com.jernejerin.traffic.helper.PollingDriver;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.Environment;
import reactor.core.DispatcherSupplier;
import reactor.fn.tuple.Tuple;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * An example of single threaded event driven architecture - EDA.
 * </p>
 *
 * @author Jernej Jerin
 */
public class SEDA extends Architecture {
    // initial table size is 1e5
    private static Map<Route, Route> routes = new LinkedHashMap<>(100000);

    private static Map<Cell, Cell> cells = new LinkedHashMap<>(100000);

    // current top 10 sorted
    private static LinkedList<Route> top10PreviousQuery1 = new LinkedList<>();
    private static LinkedList<Cell> top10PreviousQuery2 = new LinkedList<>();

    /** The default number of threads for stage 1. */
    private static int stage1T = 1;

    /** The default number of threads for stage 2. */
    private static int stage2T = 1;

    static int id = 0;

    DispatcherSupplier supplierStage1;
    DispatcherSupplier supplierStage2;

    private final static Logger LOGGER = Logger.getLogger(SEDA.class.getName());

    public SEDA(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
        // suppliers for stages
        this.supplierStage1 = Environment.newCachedDispatchers(stage1T, "stage1");
        this.supplierStage2 = Environment.newCachedDispatchers(stage2T, "stage2");
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting SEDA solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder().fileNameQuery1Output("output/query/" +
                SEDA.class.getSimpleName() + "_query1.txt").fileNameQuery2Output("output/query/" +
                SEDA.class.getSimpleName() + "_query2.txt");

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the SEDA solution using the builder
        SEDA seda = new SEDA(builder);

        // Then we set up and register the PoolingDriver.
        LOGGER.log(Level.INFO, "Registering pooling driver from thread = " + Thread.currentThread());
        try {
            PollingDriver.setupDriver("jdbc:mysql://" + seda.hostDB + ":" + seda.portDB + "/" +
                    seda.schemaDB, seda.userDB, seda.passDB);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // run the solution
        seda.run();
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

        // synchronization signal
        CountDownLatch completeSignal = new CountDownLatch(1);

        // processing through stages of streams, where each stage has a separate thread pool
        // for processing streams. We basically fork the stream in each stage to number of streams,
        // which equals the number of threads in pool
        Stream<Trip> sharedTripsStream = taxiStream.getTrips()
            .map(t -> Tuple.of(t, System.currentTimeMillis(), id++))
            // parallelize stream tasks to separate streams for stage 1 - PARSING AND FILTERING INCORRECT DATA
            .partition(stage1T)
            // we receive streams grouped by accordingly to the positive modulo of the
            // current hashcode with respect to the number of buckets specified
            .flatMap(stream -> stream
                            // use dispatcher pool to assign to the newly generated streams
                            .dispatchOn(supplierStage1.get())
                                    // stage 1 is for validating ticket structure and filtering
                            .map(t -> TripOperations.parseValidateTrip(t.getT1(), t.getT2(), t.getT3()))
                                    // filter invalid data
                            .filter(t -> t != null & t.getRoute250() != null)
            )
            // stage 2 consists of storing the trip into DB
            .partition(stage2T)
            .flatMap(stream -> stream
                            .dispatchOn(supplierStage2.get())
                            .map(trip -> {
                                // insert into DB
                                TripOperations.insertTrip(trip);
                                return trip;
                            })
            )
            // dispatcher for funneling/joining result back to single thread
            .dispatchOn(Environment.sharedDispatcher())
            .broadcast();

        // query 1: Frequent routes
        sharedTripsStream
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
                    } else {
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
                .observeComplete(v -> completeSignal.countDown())
                .consume(ct -> {
                    if (!ct.getT1().equals(top100Routes.subList(0, top100Routes.size() < 10 ?
                            top100Routes.size() : 10))) {
                        writeTop10ChangeQuery1(top100Routes.subList(0, top100Routes.size() < 10 ?
                                        top100Routes.size() : 10), ct.getT2(), ct.getT3(), ct.getT4(),
                                ct.getT5());
                    }
                });

        // read the stream from file: for local testing
        taxiStream.readStream();

        // wait for onComplete event
        completeSignal.await();
        id = 0;

        // compute the time that was needed to get the solution
        return System.currentTimeMillis() - startTime;
    }
}
