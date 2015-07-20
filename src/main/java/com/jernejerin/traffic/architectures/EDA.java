package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.entities.*;
import com.jernejerin.traffic.client.TaxiStream;
import com.jernejerin.traffic.helper.MedianOfStream;
import com.jernejerin.traffic.helper.PollingDriver;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.fn.tuple.Tuple;
import reactor.rx.Stream;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * An example of a single threaded event driven architecture - EDA.
 * This solution consists of LinkedHashMap.
 *
 * @author Jernej Jerin
 */
public class EDA extends Architecture {
    private final static Logger LOGGER = Logger.getLogger(EDA.class.getName());
    private static int id = 0;

    // controls the size of the list to store largest Routes
    // TODO (Jernej Jerin): From empirical testing it seems that
    // TODO (Jernej Jerin): the best n is at value 30.
    private int n = 100;

    public EDA(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
    }

    public EDA(ArchitectureBuilder builder, int n) {
        // call super constructor to initialize fields from builder
        super(builder);
        this.n = n;
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting single threaded EDA 3 solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder().fileNameQuery1Output("output/query/" +
                EDA.class.getSimpleName() + "_query1.txt").fileNameQuery2Output("output/query/" +
                EDA.class.getSimpleName() + "_query2.txt");

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the EDA solution using the builder
        Architecture eda = new EDA(builder);

        // run the solution
        eda.run();
    }

    public long run() throws InterruptedException {
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

        // current top N sorted
        final LinkedList<RouteCount> topNRoutes = new LinkedList<>();
        final LinkedList<CellProfitability> topNCellProfitability = new LinkedList<>();
        final List<CellProfitability> top10Cells = new LinkedList<>();

        // time windows
        final ArrayDeque<Trip> trips = new ArrayDeque<>();
        ArrayDeque<Trip> tripProfits = new ArrayDeque<>();
        ArrayDeque<Trip> tripEmptyTaxis = new ArrayDeque<>();

        // key value storage
        final LinkedHashMap<Route, RouteCount> routesCount = new LinkedHashMap<>(100000);
        final LinkedHashMap<Cell, EmptyTaxisCount> emptyTaxis = new LinkedHashMap<>(100000);
        final LinkedHashMap<Cell, CellProfit> cellProfits = new LinkedHashMap<>(100000);



        CountDownLatch completeSignal = new CountDownLatch(2);

        Stream<Trip> sharedTripsStream = taxiStream.getTrips()
                .map(t -> Tuple.of(t, System.currentTimeMillis(), id++))
                .map(t -> TripOperations.parseValidateTrip(t.getT1(), t.getT2(), t.getT3()))
                .filter(t -> t != null && t.getRoute250() != null)
//                .map(t -> {
//                    TripOperations.insertTrip(t);
//                    return t;
//                })
                .broadcast();

        // query 1: Frequent routes
        sharedTripsStream
                .observeComplete(v -> completeSignal.countDown())
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

                        // if route is in top N, then resort the top N
                        // if it is not in top N, then we do not have to do
                        // anything because counting down the route count
                        // will not change the top N
                        int i = topNRoutes.indexOf(routeCount);
                        if (i != -1) {
                            List<RouteCount> top10 = null;

                            // save current top 10 for future comparison only if
                            // the route of the removed trip lies in top 10
                            if (i <= 9) {
                                top10 = new LinkedList<>(topNRoutes.subList(0, topNRoutes.size() < 10 ?
                                        topNRoutes.size() : 10));
                            }

                            // flip elements from i-th to the last one
                            // until the current route is smaller
                            for (int j = i, k = i + 1; k < topNRoutes.size(); j++, k++) {
                                // route count at current position is smaller, then switch places
                                if (topNRoutes.get(j).compareTo(topNRoutes.get(k)) < 0) {
                                    Collections.swap(topNRoutes, j, k);
                                }
                            }

                            // if the element falls into the last place in top N, then we need to
                            // resort all route count
                            if (topNRoutes.get(topNRoutes.size() - 1).equals(routeCount)) {
                                // a priority queue for top N routes. Orders by natural number.
                                BoundedPriorityQueue<RouteCount> newTopNRoutes = new BoundedPriorityQueue<>
                                        (Comparator.<RouteCount>naturalOrder(), this.n);

                                // try to add each route count to the top N list. This way we get sorted top N with
                                // time complexity n * log(N) + N * log(N) vs. n * log(n), where N << n!
                                routesCount.forEach((r, rc) -> newTopNRoutes.offer(rc));

                                // copy to the top N routes and resort (N * log(N))
                                Collections.copy(topNRoutes, new LinkedList<>(newTopNRoutes));
                                Collections.sort(topNRoutes, Comparator.<RouteCount>reverseOrder());
                            }

                            // check if top 10 has changed
                            if (top10 != null && !top10.equals(topNRoutes.subList(0, topNRoutes.size() < 10 ?
                                    topNRoutes.size() : 10))) {
                                writeTop10ChangeQuery1(topNRoutes.subList(0, topNRoutes.size() < 10 ? topNRoutes.size() : 10),
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
                    LinkedList<RouteCount> top10 = new LinkedList<>(topNRoutes.subList(0, topNRoutes.size() < 10 ?
                            topNRoutes.size() : 10));

                    int i = topNRoutes.indexOf(routeCount);
                    if (i != -1) {
                        topNRoutes.set(i, routeCount);
                        // flip elements from i-th to the first one
                        // until the current route is larger
                        for (int j = i - 1, k = i; j >= 0; j--, k--) {
                            // route count at current position is smaller, then switch places
                            if (topNRoutes.get(j).compareTo(topNRoutes.get(k)) < 0) {
                                Collections.swap(topNRoutes, j, k);
                            }
                        }
                    } else {
                        // check top N size
                        if (topNRoutes.size() < this.n) {
                            topNRoutes.addLast(routeCount);
                        }
                        // compare it with the last element in top 100
                        else if (topNRoutes.getLast().compareTo(routeCount) < 0) {
                            topNRoutes.set(this.n - 1, routeCount);
                        }

                        for (int j = topNRoutes.size() - 2, k = topNRoutes.size() - 1; j >= 0; j--, k--) {
                            // route count at current position is smaller, then switch places
                            if (topNRoutes.get(j).compareTo(topNRoutes.get(k)) < 0) {
                                Collections.swap(topNRoutes, j, k);
                            }
                        }
                    }
                    return Tuple.of(top10, t.getPickupDatetime(), t.getDropOffDatetime(),
                            t.getTimestampReceived(), t);

                })
                .consume(ct -> {
                    if (!ct.getT1().equals(topNRoutes.subList(0, topNRoutes.size() < 10 ?
                            topNRoutes.size() : 10))) {
                        writeTop10ChangeQuery1(topNRoutes.subList(0, topNRoutes.size() < 10 ?
                                        topNRoutes.size() : 10), ct.getT2(), ct.getT3(), ct.getT4(),
                                ct.getT5());
                    }
                });

        // query 2: Profitable cells
        sharedTripsStream
                .observeComplete(v -> completeSignal.countDown())
                .map(t -> {
                    while (tripEmptyTaxis.peek() != null && tripEmptyTaxis.peek().getDropOffTimestamp() <
                            t.getDropOffTimestamp() - 30 * 60 * 1000) {
                        // a priority queue for top 10 cells. Orders by natural number.
                        BoundedPriorityQueue<CellProfitability> top10 = new BoundedPriorityQueue<>(Comparator.<CellProfitability>naturalOrder(), 10);

                        Trip trip = tripEmptyTaxis.poll();

                        // update the empty taxis count for the end cell of the trip, leaving the window
                        EmptyTaxisCount emptyTaxisCount = emptyTaxis.get(trip.getRoute250().getEndCell());
                        emptyTaxisCount.setCount(emptyTaxisCount.getCount() - 1);

//                        int i = -1;
//                        for (int j = 0; j < topNCellProfitability.size(); j++) {
//                            if (topNCellProfitability.get(j).getCell().equals(trip.getRoute250().getEndCell())) {
//                                i = j;
//                                break;
//                            }
//                        }

                        // if count is 0 then remove the empty taxis to avoid unnecessary iteration
                        if (emptyTaxisCount.getCount() == 0) {
                            emptyTaxis.remove(emptyTaxisCount);

                            // also if it exists in topN profitable cells, remove it because the cells
                            // with 0 empty taxis should not be taken into account
//                            if (i != -1) {
//                                topNCellProfitability.remove(i);
//
//                                // we need to resort all profitable cells
//                            }
                        } else {
                            emptyTaxis.put(trip.getRoute250().getEndCell(), emptyTaxisCount);

                            // if it exists in topN profitable cells, then we need to check it its profitability
                            // has decreased so much that the next profitable cell in line will be greater
                        }

                        // recompute all profitable cells
                        // only consider those cells that have empty taxis
                        emptyTaxis.forEach((ec, etc) -> {
                            // get the profit for current end cell
                            CellProfit endCellProfit = cellProfits.get(ec);
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

                        // if there is change in top 10, write it
                        if (!top10Cells.equals(top10SortedNew)) {
                            top10Cells.clear();
                            top10Cells.addAll(top10SortedNew);
                            writeTop10ChangeQuery2(top10SortedNew, trip.getPickupDatetime().plusMinutes(30),
                                    trip.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived());
                        }

                    }
                    while (tripProfits.peek() != null && tripProfits.peek().getDropOffTimestamp() <
                            t.getDropOffTimestamp() - 15 * 60 * 1000) {
                        // a priority queue for top 10 cells. Orders by natural number.
                        BoundedPriorityQueue<CellProfitability> top10 = new BoundedPriorityQueue<>(Comparator.<CellProfitability>naturalOrder(), 10);

                        Trip trip = tripProfits.poll();

                        // update the empty taxis count for the end cell of the trip, leaving the window
                        CellProfit cellProfit = cellProfits.get(trip.getRoute250().getStartCell());
                        cellProfit.getMedianProfit().removeNumberFromStream(trip.getFareAmount() + trip.getTipAmount());

                        // if there is no profit on cell, then remove it
                        if (cellProfit.getMedianProfit().numOfElements == 0) {
                            cellProfits.remove(cellProfit);
                        } else {
                            cellProfits.put(trip.getRoute250().getStartCell(), cellProfit);
                        }

                        // recompute all profitable cells
                        // only consider those cells that have empty taxis
                        emptyTaxis.forEach((ec, etc) -> {
                            // get the profit for current end cell
                            CellProfit endCellProfit = cellProfits.get(ec);
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

                        // if there is change in top 10, write it
                        if (!top10Cells.equals(top10SortedNew)) {
                            top10Cells.clear();
                            top10Cells.addAll(top10SortedNew);
                            writeTop10ChangeQuery2(top10SortedNew, trip.getPickupDatetime().plusMinutes(30),
                                    trip.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived());
                        }
                    }

                    // store trip in both windows
                    tripEmptyTaxis.add(t);
                    tripProfits.add(t);

                    // update map for empty taxi count
                    EmptyTaxisCount emptyTaxisCount = emptyTaxis.getOrDefault(t.getRoute250().getEndCell(),
                            new EmptyTaxisCount(t.getId(), 0));
                    emptyTaxisCount.setCount(emptyTaxisCount.getCount() + 1);
                    emptyTaxisCount.setId(t.getId());
                    emptyTaxis.put(t.getRoute250().getEndCell(), emptyTaxisCount);

                    // update map for cell profits
                    CellProfit cellProfit = cellProfits.getOrDefault(t.getRoute250().getStartCell(),
                            new CellProfit(t.getId(), new MedianOfStream<Float>()));
                    cellProfit.getMedianProfit().addNumberToStream(t.getFareAmount() + t.getTipAmount());
                    cellProfit.setId(t.getId());
                    cellProfits.put(t.getRoute250().getStartCell(), cellProfit);

                    // update the profit and trip id for the cell profitability of the incoming trip
                    // if the cell profitability does not yet exist, create a new one
                    // TODO (Jernej Jerin): How do we store cell profitability?
//                    CellProfitability cellProfitability = routesCount.getOrDefault(t.getRoute500(),
//                            new RouteCount(t.getRoute500(), t.getId(), 0));
//                    routeCount.setCount(routeCount.getCount() + 1);
//                    routeCount.setId(t.getId());
//                    routesCount.put(t.getRoute500(), routeCount);

                    return t;
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
