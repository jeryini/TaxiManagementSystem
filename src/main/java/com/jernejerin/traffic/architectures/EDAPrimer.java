package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.entities.Cell;
import com.jernejerin.traffic.entities.Route;
import com.jernejerin.traffic.entities.RouteCount;
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
public class EDAPrimer extends Architecture {
    private final static Logger LOGGER = Logger.getLogger(EDAPrimer.class.getName());

    public EDAPrimer(ArchitectureBuilder builder) {
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
        Architecture edaPrimer = new EDAPrimer(builder);

        // run the solution
        edaPrimer.run();
    }

    public long run() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // create a taxi service
        this.taxiStream = new TaxiStream("/com/jernejerin/" + this.fileNameInput);

        // current top 10 sorted
        final List<RouteCount> top10Routes = new LinkedList<>();
        final List<Cell> top10Cells = new LinkedList<>();
        CountDownLatch completeSignal = new CountDownLatch(1);
        Queue<Route> routes = new ArrayDeque<>();
        ArrayDeque<Route> profitCells = new ArrayDeque<>();
        ArrayDeque<Route> emptyTaxiCells = new ArrayDeque<>();


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
                                .groupBy(t -> t.getRoute() != null)
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
                    while (routes.peek() != null && routes.peek().getDropOffDatetime().isBefore(
                            t.getDropOffDatetime().minusMinutes(30))) {
                        Route route = routes.poll();
                        List<RouteCount> bestRoutes = bestRoutes(routes);
                        // if there is change in top 10, write it
                        if (!top10Routes.equals(bestRoutes)) {
                            top10Routes.clear();
                            top10Routes.addAll(bestRoutes);
                            writeTop10ChangeQuery1(bestRoutes, route.getPickupDatetime().plusMinutes(30),
                                    route.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived());
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

        // query 2: Frequent routes
        taxiStream.query2
                .map(t -> {
                    // events leaving window for empty taxis
                    while (emptyTaxiCells.peek() != null && emptyTaxiCells.peek().getDropOffDatetime().isBefore(
                            t.getDropOffDatetime().minusMinutes(30))) {
                        Route route = emptyTaxiCells.poll();

                        List<Cell> bestCells = bestCells(emptyTaxiCells, profitCells);
                        // if there is change in top 10, write it
                        if (!top10Cells.equals(bestCells)) {
                            top10Cells.clear();
                            top10Cells.addAll(bestCells);
                            writeTop10ChangeQuery2(bestCells, route.getPickupDatetime().plusMinutes(30),
                                    route.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived());
                        }

                    }
                    // events leaving the window for profit cells
                    while (profitCells.peek() != null && profitCells.peek().getDropOffDatetime().isBefore(
                            t.getDropOffDatetime().minusMinutes(15))) {
                        Route route = profitCells.poll();

                        List<Cell> bestCells = bestCells(emptyTaxiCells, profitCells);
                        // if there is change in top 10, write it
                        if (!top10Cells.equals(bestCells)) {
                            top10Cells.clear();
                            top10Cells.addAll(bestCells);
                            writeTop10ChangeQuery2(bestCells, route.getPickupDatetime().plusMinutes(30),
                                    route.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived());
                        }
                    }

                    t.getRoute().profit = t.getFareAmount() + t.getTipAmount();

                    // add to window
                    profitCells.add(t.getRoute());
                    emptyTaxiCells.add(t.getRoute());

                    List<Cell> bestCells = bestCells(emptyTaxiCells, profitCells);
                    return Tuple.of(bestCells, t.getPickupDatetime(), t.getDropOffDatetime(),
                            t.getTimestampReceived());
                })
                .consume(ct -> {
                    if (!top10Cells.equals(ct.getT1())) {
                        top10Cells.clear();
                        top10Cells.addAll(ct.getT1());
                        writeTop10ChangeQuery2(ct.getT1(), ct.getT2(), ct.getT3(), ct.getT4());
                    }
                });

        // read the stream from file: for local testing
        taxiStream.readStream();

        // wait for onComplete event
        completeSignal.await();
        return System.currentTimeMillis() - startTime;
    }


    private List<RouteCount> bestRoutes(Queue<Route> routes) {
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

    private List<Cell> bestCells(ArrayDeque<Route> emptyTaxiCells, Queue<Route> profitCells) {
        // a priority queue for top 10 cells. Orders by natural number.
        BoundedPriorityQueue<Cell> top10 = new BoundedPriorityQueue<>(Comparator.<Cell>naturalOrder(), 10);

        // for empty taxis we need reverse as the most recent unique taxi medallion matters
        Map<Cell, RouteCount> emptyTaxis = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                emptyTaxiCells.descendingIterator(), Spliterator.ORDERED), false)
                // filter by distinct medallion, this is unique taxi identifier
                // we want only the most recent unique empty taxis
                .filter(distinctByKey(r -> r.medallion))
                // group by end cell
                .collect(Collectors.groupingBy(route -> route.getEndCell(),
                                Collectors.collectingAndThen(
                                        Collectors.mapping(RouteCount::fromRoute, Collectors.reducing(RouteCount::combine)),
                                        Optional::get
                                )
                        )
                );
        Map<Cell, RouteCount> profits = profitCells.stream()
                // profit for starting cell, so we need to group by start cell
                .collect(Collectors.groupingBy(route -> route.getStartCell(),
                                Collectors.collectingAndThen(
                                        Collectors.mapping(RouteCount::fromRouteProfit, Collectors.reducing(RouteCount::combineByProfit)),
                                        Optional::get
                                )
                        )
                );
        emptyTaxis.forEach((c, rc) -> {
            c.profitability = profits.get(c).medianProfit.getMedian() / rc.count;
            top10.add(c);
        });

        List<Cell> top10SortedNew = new LinkedList<>(top10);
        // sort routes
        top10SortedNew.sort(Comparator.<Cell>reverseOrder());

        return top10SortedNew;

    }

    public static <T> Predicate<T> distinctByKey(Function<? super T,Object> keyExtractor) {
        Map<Object,Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }
}
