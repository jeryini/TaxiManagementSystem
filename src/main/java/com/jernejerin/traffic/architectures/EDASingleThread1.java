package com.jernejerin.traffic.architectures;

import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.entities.Cell;
import com.jernejerin.traffic.entities.Route;
import com.jernejerin.traffic.entities.Taxi;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.rx.Streams;

import java.io.FileOutputStream;
import java.io.IOException;
import java.time.ZoneOffset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * An example of a single threaded event driven architecture - EDA.
 * This solution consists of LinkedHashMap.
 *
 * @author Jernej Jerin
 */
public class EDASingleThread1 extends Architecture {
    private final static Logger LOGGER = Logger.getLogger(EDASingleThread1.class.getName());

    // current top 10 sorted
    static LinkedList<Route> top10Routes = new LinkedList<>();
    static LinkedList<Cell> top10Cells = new LinkedList<>();

    public EDASingleThread1(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting single threaded EDA 1 solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder();

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the EDA solution using the builder
        EDASingleThread1 eda1 = new EDASingleThread1(builder);

        // run the solution
        eda1.run();
    }

    public void run() throws InterruptedException {
        // initial table size is 1e5
        final Map<Route, Route> routes = new LinkedHashMap<>(100000);
        final Map<Cell, Cell> cells = new LinkedHashMap<>(100000);

        // consumer for TCP server
        this.serverTCP.start(ch -> {
            ch.log("conn").consume(trip -> {
                LOGGER.log(Level.INFO, "TCP server receiving trip " +
                        trip + " from thread = " + Thread.currentThread());
                // dispatch event to a broadcaster pipeline
                this.taxiStream.getTrips().onNext(trip);
                LOGGER.log(Level.INFO, "TCP server send ticket to streaming pipeline for ticket = " +
                        trip + " from thread = " + Thread.currentThread());
            });
            return Streams.never();
        }).await();

        // processing through streams with only 1 thread
        taxiStream.getTrips()
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
                                                    // a priority queue for top 10 routes. Orders by natural number.
                                                    BoundedPriorityQueue<Route> top10 = new BoundedPriorityQueue<>(Comparator.<Route>naturalOrder(), 10);

                                                    long dropOffTimestamp = t.getDropOffDatetime().toEpochSecond(ZoneOffset.UTC) * 1000;

                                                    // check for all the routes if there are any events leaving the window.
                                                    // This is done by comparing current trip drop off time minus 30 min
                                                    // with the head of the drop off for each route
                                                    routes.forEach((k, r) -> {
                                                        while (r.getDropOffWindow().peek() != null && r.getDropOffWindow().peek() <
                                                                dropOffTimestamp - 30 * 60 * 1000) {
                                                            // event leaving the window
                                                            r.getDropOffWindow().poll();

                                                            // update drop off size of the key. We can do that, as
                                                            // hash code for the key does not contain attribute drop off size!
                                                            r.setDropOffSize(r.getDropOffSize() - 1);
                                                        }

                                                        // if the drop off window is empty, then we can remove the element from
                                                        // route map. This way we are only maintaining a map of routes


                                                        // active in last 30 minutes
                                                        if (r.getDropOffWindow().peek() != null)
//                                                routes.remove(k);
                                                            // try to add it to top 10 list. This way we get sorted top 10 with
                                                            // time complexity n * log(10) + 10 * log(10) vs. n * log(n)
//                                            else
                                                            top10.offer(r);
                                                    });

                                                    // get value in map or default value which is current route
                                                    Route route = routes.getOrDefault(t.getRoute(), t.getRoute());

                                                    // set the latest timestamp
                                                    route.getDropOffWindow().add(dropOffTimestamp);
                                                    route.setDropOffSize(route.getDropOffSize() + 1);
                                                    route.setLastUpdated(System.currentTimeMillis());

                                                    // put if the route was not in the hash
                                                    if (route == t.getRoute())
                                                        routes.put(route, route);

                                                    // try to add it to top 10 of the frequent routes.
                                                    // These are sorted by drop off size and latest drop off window timestamp
                                                    top10.remove(route);
                                                    top10.offer(route);

                                                    // sort new top 10 routes
                                                    LinkedList<Route> top10SortedNew = new LinkedList<>(top10);
                                                    top10SortedNew.sort(Comparator.<Route>reverseOrder());

                                                    // do they contain the same elements in the same order?
                                                    // routes are equal if they have the same start and end cell
                                                    boolean changed = !top10SortedNew.equals(top10Routes);
                                                    top10Routes = top10SortedNew;

                                                    return Tuple.of(changed, t.getPickupDatetime(), t.getDropOffDatetime(),
                                                            top10SortedNew, t.getTimestampReceived(), t);
                                                })
//                                 query 2: Profitable areas
                                                .map(ct -> {
                                                    // a priority queue for top 10 cells. Orders by natural number.
                                                    BoundedPriorityQueue<Cell> top10 = new BoundedPriorityQueue<>(Comparator.<Cell>naturalOrder(), 10);

                                                    long dropOffTimestamp = ct.getT6().getDropOffDatetime().toEpochSecond(ZoneOffset.UTC) * 1000;

                                                    // get value in map or default value which is current drop off cell
                                                    Cell endCell = cells.getOrDefault(ct.getT6().getRoute().getEndCell(),
                                                            ct.getT6().getRoute().getEndCell());

                                                    // put the latest taxi with medallion
                                                    endCell.getTaxis().add(new Taxi(dropOffTimestamp, ct.getT6().getMedallion()));

                                                    // for updating profit we need the start cell of the trip
                                                    Cell startCell = cells.getOrDefault(ct.getT6().getRoute().getStartCell(),
                                                            ct.getT6().getRoute().getStartCell());

                                                    Integer profit = (int) (ct.getT6().getFareAmount() * 100 +
                                                            ct.getT6().getTipAmount() * 100);

                                                    // add to trip profit time
                                                    startCell.getTripProfitTime().add(Tuple.of(profit, dropOffTimestamp));

                                                    // and add to median profit
                                                    startCell.getMedianProfit().addNumberToStream(profit);

                                                    // put if the end cell was not in cells
                                                    if (endCell == ct.getT6().getRoute().getEndCell())
                                                        cells.put(endCell, endCell);

                                                    // put if the start cell was not in cells
                                                    if (startCell == ct.getT6().getRoute().getStartCell())
                                                        cells.put(startCell, startCell);

                                                    // check each cell
                                                    cells.forEach((k, c) -> {
                                                        Iterator<Taxi> taxiIterator = c.getTaxis().iterator();
                                                        while (taxiIterator.hasNext()) {
                                                            Taxi taxi = taxiIterator.next();
                                                            // taxis within the last 30 minutes
                                                            if (taxi.getDropOffTimestamp() < dropOffTimestamp - 30 * 60 * 1000) {
                                                                // remove the taxi from this cell
                                                                taxiIterator.remove();
                                                            } else
                                                                break;
                                                        }

                                                        // check for trips that started in the area more than 15 minutes ago
                                                        // and remove them. Also remove the profit from median profit
                                                        while (c.getTripProfitTime().peek() != null && c.getTripProfitTime().peek().
                                                                getT2() < dropOffTimestamp - 15 * 60 * 1000) {
                                                            // trip event leaving the window
                                                            Tuple2<Integer, Long> trip = c.getTripProfitTime().poll();

                                                            // remove the profit from median profit
                                                            c.getMedianProfit().removeNumberFromStream(trip.getT1());
                                                        }

                                                        // only set profitability and add it to top 10 if there are some
                                                        // empty taxis in this cell and if the cell has some elements in median
                                                        // profit. This means that at least one trip has ended in the last 15
                                                        // minutes, which start cell was current cell
                                                        if (!c.getTaxis().isEmpty() && c.getMedianProfit().numOfElements > 0) {
                                                            // first compute profitability. We need to divide by 100 as we store
                                                            // cents into median profit
                                                            c.setProfitability((c.getMedianProfit().getMedian() / c.getTaxis().size()) / 100);

                                                            // try to add it to top 10 list. This way we get sorted top 10 with
                                                            // time complexity n * log(10) + 10 * log(10) vs. n * log(n)
                                                            top10.offer(c);
                                                        }
                                                    });

                                                    // sort new top 10 cells
                                                    LinkedList<Cell> top10SortedNew = new LinkedList<>(top10);
                                                    top10SortedNew.sort(Comparator.<Cell>reverseOrder());

                                                    // do they contain the same elements in the same order?
                                                    // cells are equal if they have the same east and south coordinates
                                                    boolean changed = !top10SortedNew.equals(top10Cells);
                                                    top10Cells = top10SortedNew;

                                                    return Tuple.of(ct, Tuple.of(changed, ct.getT6().getPickupDatetime(),
                                                            ct.getT6().getDropOffDatetime(), top10SortedNew));
                                                })
                                                .consume(ct -> {
                                                    if (ct.getT1().getT1()) {
                                                        // build content string for output
                                                        String content = ct.getT2().toString() + ", " + ct.getT1().getT3().toString() + ", ";

                                                        // iterate over all the most frequent routes
                                                        for (Route route : ct.getT1().getT4()) {
                                                            content += route.getStartCell().getEast() + "." + route.getStartCell().getSouth() +
                                                                    ", " + route.getEndCell().getEast() + "." + route.getEndCell().getSouth() + " (" + route.getDropOffSize() + "), ";
                                                        }

                                                        // add a delay
                                                        content += System.currentTimeMillis() - ct.getT1().getT5() + "\n";

                                                        try (FileOutputStream fop = new FileOutputStream(fileQuery1, true)) {
                                                            // write to file
                                                            fop.write(content.getBytes());
                                                            fop.flush();
                                                            fop.close();
                                                        } catch (IOException ex) {
                                                            LOGGER.log(Level.SEVERE, ex.getMessage());
                                                        }
                                                    }
//                                 write to file stream for query 1 if the top 10 queue was changed
                                                    if (ct.getT1().getT1()) {
                                                        // build content string for output
                                                        String content = ct.getT1().getT2().toString() + ", " + ct.getT1().getT3().toString() + ", ";

                                                        // iterate over all the most frequent routes
                                                        for (Route route : ct.getT1().getT4()) {
                                                            content += route.getStartCell().getEast() + "." + route.getStartCell().getSouth() +
                                                                    ", " + route.getEndCell().getEast() + "." + route.getEndCell().getSouth() + " (" + route.getDropOffSize() + "), ";
                                                        }

                                                        // add a delay
                                                        content += System.currentTimeMillis() - ct.getT1().getT5() + "\n";

                                                        try (FileOutputStream fop = new FileOutputStream(fileQuery1, true)) {
                                                            // write to file
                                                            fop.write(content.getBytes());
                                                            fop.flush();
                                                            fop.close();
                                                        } catch (IOException ex) {
                                                            LOGGER.log(Level.SEVERE, ex.getMessage());
                                                        }
                                                    }

//                                 write to file stream for query 2 if the top 10 queue was changed
                                                    if (ct.getT2().getT1()) {
                                                        // build content string for output
                                                        String content = ct.getT2().getT2().toString() + ", " + ct.getT2().getT3().toString() + ", ";

                                                        // iterate over all the most profitable cells
                                                        for (Cell cell : ct.getT2().getT4()) {
                                                            content += cell.getEast() + "." + cell.getSouth() +
                                                                    ", " + cell.getTaxis().size() + ", " + cell.getMedianProfit().
                                                                    getMedian() / 100 + " (" + cell.getMedianProfit().numOfElements + "), " + cell.getProfitability() + ", ";
                                                        }

                                                        // add a delay
                                                        content += System.currentTimeMillis() - ct.getT1().getT5() + "\n";

                                                        try (FileOutputStream fop = new FileOutputStream(fileQuery2, true)) {
                                                            // write to file
                                                            fop.write(content.getBytes());
                                                            fop.flush();
                                                            fop.close();
                                                        } catch (IOException ex) {
                                                            LOGGER.log(Level.SEVERE, ex.getMessage());
                                                        }
                                                    }
                                                });
                                    }else {
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
}
