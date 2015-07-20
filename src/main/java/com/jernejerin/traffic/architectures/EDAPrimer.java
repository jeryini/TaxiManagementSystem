package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.entities.*;
import com.jernejerin.traffic.client.TaxiStream;
import com.jernejerin.traffic.helper.PollingDriver;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.fn.tuple.Tuple;
import reactor.rx.Stream;

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
 * An example of a simple, i.e. basic implementation of event driven architecture - EDA.
 * This solution represents the correct, i.e. a primer implementation for query 1 and query 2 and is
 * therefore used as a basis for comparison of correct output of other solutions, such as
 * EDA, SEDA, AEDA, ASEDA and DASEDA.
 *
 * @author Jernej Jerin
 */
public class EDAPrimer extends Architecture {
    private final static Logger LOGGER = Logger.getLogger(EDAPrimer.class.getName());

    // counter for received trip events
    private static int id = 0;

    public EDAPrimer(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting single threaded EDA Primer solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder().fileNameQuery1Output("output/query/" +
                EDAPrimer.class.getSimpleName() + "_query1.txt").fileNameQuery2Output("output/query/" +
                EDAPrimer.class.getSimpleName() + "_query2.txt");

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the EDA solution using the builder
        Architecture edaPrimer = new EDAPrimer(builder);

        // run the solution
        edaPrimer.run();
    }

    /**
     * Implementation of basic EDA solution. As a side effect it generates two files,
     * one file for each query output.
     *
     * @return execution time to compute the solution
     * @throws InterruptedException
     */
    public long run() throws InterruptedException {
        // set up and register the PoolingDriver
        LOGGER.log(Level.INFO, "Registering pooling driver from thread = " + Thread.currentThread());
        try {
            PollingDriver.setupDriver("jdbc:mysql://" + this.hostDB + ":" + this.portDB + "/" +
                    this.schemaDB, this.userDB, this.passDB);
        } catch (Exception e) {
            e.printStackTrace();
        }

        long startTime = System.currentTimeMillis();

        // create a taxi service
        this.taxiStream = new TaxiStream("/com/jernejerin/" + this.fileNameInput);

        // current top 10 sorted
        final List<RouteCount> top10Routes = new LinkedList<>();
        final List<CellProfitability> top10Cells = new LinkedList<>();

        // time windows
        Queue<Trip> trips = new ArrayDeque<>();
        ArrayDeque<Trip> tripProfits = new ArrayDeque<>();
        ArrayDeque<Trip> tripEmptyTaxis = new ArrayDeque<>();

        // synchronization signal
        CountDownLatch completeSignal = new CountDownLatch(2);

        // sharing an upstream pipeline and wiring up 2 downstream pipelines
        Stream<Trip> sharedTripsStream = taxiStream.getTrips()
                // create a tuple of string trip, current time for computing delay
                // and id of the event. As this is our entry point it is appropriate to
                // start the time here, before any parsing is being done. This also in
                // record with the Grand challenge recommendation
                .map(t -> Tuple.of(t, System.currentTimeMillis(), id++))
                // parsing and validating trip structure
                .map(t -> TripOperations.parseValidateTrip(t.getT1(), t.getT2(), t.getT3()))
                // filter invalid data
                .filter(t -> t != null && t.getRoute250() != null)
                // insert record into DB
//                .map(t -> {
//                    TripOperations.insertTrip(t);
//                    return t;
//                })
                // wiring up 2 downstream pipelines
                .broadcast();

        // query 1: Frequent routes
        sharedTripsStream
                // observe for onComplete event and count down synchronization signal for
                // two query streams
                .observeComplete(v -> completeSignal.countDown())
                .map(t -> {
                    // trips leaving the window
                    while (trips.peek() != null && trips.peek().getDropOffTimestamp() < t.getDropOffTimestamp()
                            - 30 * 60 * 1000) {
                        Trip trip = trips.poll();
                        List<RouteCount> bestRoutes = bestRoutes(trips);

                        // if there is change in top 10, write it
                        if (!top10Routes.equals(bestRoutes)) {
                            top10Routes.clear();
                            top10Routes.addAll(bestRoutes);
                            writeTop10ChangeQuery1(bestRoutes, trip.getPickupDatetime().plusMinutes(30),
                                    trip.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived(), trip);
                        }
                    }

                    // add to window
                    trips.add(t);

                    List<RouteCount> bestRoutes = bestRoutes(trips);
                    return Tuple.of(bestRoutes, t.getPickupDatetime(), t.getDropOffDatetime(),
                            t.getTimestampReceived(), t);
                })
                .consume(ct -> {
                    if (!top10Routes.equals(ct.getT1())) {
                        top10Routes.clear();
                        top10Routes.addAll(ct.getT1());
                        writeTop10ChangeQuery1(ct.getT1(), ct.getT2(), ct.getT3(), ct.getT4(), ct.getT5());
                    }
                });


        // query 2: Profitable cells
        sharedTripsStream
                .observeComplete(v -> completeSignal.countDown())
                .map(t -> {
                    // events leaving window for empty taxis in the last 30 minutes
                    while (tripEmptyTaxis.peek() != null && tripEmptyTaxis.peek().getDropOffTimestamp() <
                            t.getDropOffTimestamp() - 30 * 60 * 1000) {
                        Trip trip = tripEmptyTaxis.poll();

                        List<CellProfitability> bestCells = bestCells(tripEmptyTaxis, tripProfits);
                        // if there is change in top 10, write it
                        if (!top10Cells.equals(bestCells)) {
                            top10Cells.clear();
                            top10Cells.addAll(bestCells);
                            writeTop10ChangeQuery2(bestCells, trip.getPickupDatetime().plusMinutes(30),
                                    trip.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived());
                        }

                    }
                    // events leaving the window for profit cells in the last 15 minutes
                    while (tripProfits.peek() != null && tripProfits.peek().getDropOffTimestamp() <
                            t.getDropOffTimestamp() - 15 * 60 * 1000) {
                        Trip trip = tripProfits.poll();

                        List<CellProfitability> bestCells = bestCells(tripEmptyTaxis, tripProfits);
                        // if there is change in top 10, write it
                        if (!top10Cells.equals(bestCells)) {
                            top10Cells.clear();
                            top10Cells.addAll(bestCells);
                            writeTop10ChangeQuery2(bestCells, trip.getPickupDatetime().plusMinutes(30),
                                    trip.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived());
                        }
                    }

                    // add to window
                    tripEmptyTaxis.add(t);
                    tripProfits.add(t);

                    List<CellProfitability> bestCells = bestCells(tripEmptyTaxis, tripProfits);
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

        // wait for onComplete event and then reset the counter
        completeSignal.await();
        id = 0;

        // compute the time that was needed to get the solution
        return System.currentTimeMillis() - startTime;
    }


    /**
     * Computes a list of best routes from the passed list of trips.
     *
     * @param trips a list of trips that have completed in the last 30 minutes
     * @return a list of route frequency, where each route contains a frequency. The list
     * is ordered is descending order by the count attribute.
     */
    private List<RouteCount> bestRoutes(Queue<Trip> trips) {
        // a priority queue for top 10 routes. Orders by natural number.
        BoundedPriorityQueue<RouteCount> top10 = new BoundedPriorityQueue<>(Comparator.<RouteCount>naturalOrder(), 10);

        // creates a stream from a list of trips and groups them by route500. Then it maps
        // each trip to RouteCount and reduces/combines the RouteCount according to the largest id
        // of the trip
        Map<Route, RouteCount> routesCounted = trips.stream()
                .collect(Collectors.groupingBy(Trip::getRoute500,
                                Collectors.collectingAndThen(
                                        Collectors.mapping(RouteCount::fromTrip, Collectors.reducing(RouteCount::combine)),
                                        Optional::get
                                )
                        )
                );

        // pass each RouteCount to the bounded priority queue.
        routesCounted.forEach((r, rc) -> top10.offer(rc));
        List<RouteCount> top10SortedNew = new LinkedList<>(top10);

        // sort routes
        top10SortedNew.sort(Comparator.<RouteCount>reverseOrder());
        return top10SortedNew;
    }

    /**
     * Computes a list of the most profitable cells from the passed list of empty taxis and
     * profit cells.
     *
     * @param tripEmptyTaxis The number of empty taxis in an area is the sum of taxis that had a drop-off location in
     *                      that area less than 30 minutes ago and had no following pickup yet.
     * @param tripProfits The profit that originates from an area is computed by calculating the median fare + tip for
     *                    trips that started in the area and ended within the last 15 minutes.
     * @return A list of most profitable cells ordered in descending order by the cell median profit.
     */
    private List<CellProfitability> bestCells(ArrayDeque<Trip> tripEmptyTaxis, ArrayDeque<Trip> tripProfits) {
        // a priority queue for top 10 cells. Orders by natural number.
        BoundedPriorityQueue<CellProfitability> top10 = new BoundedPriorityQueue<>(Comparator.<CellProfitability>naturalOrder(), 10);

        // for empty taxis we need reverse as the most recent unique taxi medallion matters
        Map<Cell, EmptyTaxisCount> emptyTaxis = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                tripEmptyTaxis.descendingIterator(), Spliterator.ORDERED), false)
                // filter by distinct medallion, this is unique taxi identifier
                // we want only the most recent unique empty taxis
                .filter(distinctByKey(Trip::getMedallion))
                // group by end cell
                .collect(Collectors.groupingBy(t -> t.getRoute250().getEndCell(),
                                Collectors.collectingAndThen(
                                        Collectors.mapping(EmptyTaxisCount::fromTrip,
                                                Collectors.reducing(EmptyTaxisCount::combine)),
                                        Optional::get
                                )
                        )
                );
        Map<Cell, CellProfit> profits = tripProfits.stream()
                // profit for starting cell, so we need to group by start cell
                .collect(Collectors.groupingBy(trip -> trip.getRoute250().getStartCell(),
                                Collectors.collectingAndThen(
                                        Collectors.mapping(CellProfit::fromTrip,
                                        Collectors.reducing(CellProfit::combine)),
                                        Optional::get
                                )
                        )
                );

        // only consider those cells that have empty taxis
        emptyTaxis.forEach((ec, etc) -> {
            // get the profit for current end cell
            CellProfit endCellProfit = profits.get(ec);
            if (endCellProfit != null) {
                top10.offer(new CellProfitability(ec, etc.getId() > endCellProfit.getId() ? etc.getId() :
                        endCellProfit.getId(), etc.getCount(), endCellProfit.getMedianProfit().getMedian(),
                        endCellProfit.getMedianProfit().getMedian() / etc.getCount()));
            } else {
                // end cell profit does not exist, just set median profit and profitability to 0
                top10.offer(new CellProfitability(ec, etc.getId(), etc.getCount(), 0, 0));
            }
        });

        List<CellProfitability> top10SortedNew = new LinkedList<>(top10);
        // sort routes
        top10SortedNew.sort(Comparator.<CellProfitability>reverseOrder());

        return top10SortedNew;
    }

    /**
     * A predicate to return distinct values by key.
     *
     * @param keyExtractor
     * @param <T>
     * @return
     */
    public static <T> Predicate<T> distinctByKey(Function<? super T,Object> keyExtractor) {
        Map<Object,Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }
}
