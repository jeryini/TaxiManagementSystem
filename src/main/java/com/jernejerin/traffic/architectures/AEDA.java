package com.jernejerin.traffic.architectures;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import com.jernejerin.traffic.entities.*;
import com.jernejerin.traffic.client.TaxiStream;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.fn.tuple.Tuple;
import reactor.rx.Stream;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

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

        // Actor factory for creating Actors.
        final ActorSystem system = ActorSystem.create("actorFactory");

        // create an "actor-in-a-box". It contains an Actor which can be used as a puppet for sending messages
        // to other Actors and receiving their replies.
        final Inbox inbox = Inbox.create(system);
        final Inbox inboxProfitableCells = Inbox.create(system);

        // This will be our root Actor for query 1.
        final ActorRef rootTop10Routes = system.actorOf(Props.create(RouteActor.TopRouteNode.class), "rootTop10Routes");

        // root Actor for query 2
        final ActorRef rootTop10ProfitableCells = system.actorOf(Props.create(CellProfitabilityActor.TopCellNode.class), "rootTop10ProfitableCells");

        // time window for query 1
        final ArrayDeque<Trip> trips = new ArrayDeque<>();

        // time window for query 2
        ArrayDeque<Trip> tripProfits = new ArrayDeque<>();
        ArrayDeque<Trip> tripEmptyTaxis = new ArrayDeque<>();

        // top routes for query 1
        final List<RouteCount> top10Routes = new LinkedList<>();

        // top routes for query 2
        final List<CellProfitability> top10ProfitableCells = new LinkedList<>();

        CountDownLatch completeSignal = new CountDownLatch(2);

        Stream<Trip> sharedTripsStream = taxiStream.getTrips()
                .map(t -> {
                    // create a tuple of string trip and current time for computing delay
                    // As this is our entry point it is appropriate to start the time here,
                    // before any parsing is being done. This also in record with the Grand
                    // challenge recommendation
                    return Tuple.of(t, System.currentTimeMillis(), id++);
                })
                        // parsing and validating trip structure
                .map(t -> TripOperations.parseValidateTrip(t.getT1(), t.getT2(), t.getT3()))
                        // filter invalid data
                .filter(t -> t != null & t.getRoute250() != null)
                .broadcast();

        // query 1: Frequent routes
        sharedTripsStream
                .map(t -> {
                    // trips leaving the window
                    while (trips.peek() != null && trips.peek().getDropOffTimestamp() < t.getDropOffTimestamp()
                            - 30 * 60 * 1000) {
                        // remove it from queue
                        Trip trip = trips.poll();

                        // tell the root Actor to decrement the route count for the route in the removed trip
                        inbox.send(rootTop10Routes, new RouteActor.DecrementRoute(trip.getRoute500(),
                                trip.getRoute500().getId()));
                        // block until we get back the top 10
                        // TODO (Jernej Jerin): Blocking call! Should we compare for top routes in the
                        // TODO (Jernej Jerin): top actor.
                        RouteActor.Top10 routeActorTop10 = (RouteActor.Top10) inbox.receive(Duration.create(10000, "seconds"));
                        List<RouteCount> newTop10Routes = Arrays.asList(routeActorTop10.top10Routes);
                        Collections.sort(newTop10Routes, Comparator.<RouteCount>reverseOrder());

                        if (!newTop10Routes.equals(top10Routes)) {
                            top10Routes.clear();
                            top10Routes.addAll(newTop10Routes);
                            writeTop10ChangeQuery1(top10Routes, trip.getPickupDatetime().plusMinutes(30),
                                    trip.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived(), trip);
                        }
                    }

                    // add to window
                    trips.add(t);

                    // tell the root Actor to increment the route count for the route in the added trip
                    // reply should go to the inbox
                    inbox.send(rootTop10Routes, new RouteActor.IncrementRoute(t.getRoute500(), t.getRoute500().getId(), t.getId()));

                    // TODO (Jernej Jerin): Again a blocking call!
                    // TODO (Jernej Jerin): Problem with casting!
                    RouteActor.Top10 routeActorTop10 = (RouteActor.Top10) inbox.receive(Duration.create(10000, "seconds"));
                    List<RouteCount> newTop10Routes = Arrays.asList(routeActorTop10.top10Routes);
                    Collections.sort(newTop10Routes, Comparator.<RouteCount>reverseOrder());

                    return Tuple.of(newTop10Routes, t.getPickupDatetime(), t.getDropOffDatetime(),
                            t.getTimestampReceived(), t);
                })
                .observeComplete(v -> {
                    completeSignal.countDown();
                })
                .consume(ct -> {
                    if (!top10Routes.equals(ct.getT1())) {
                        top10Routes.clear();
                        top10Routes.addAll(ct.getT1());
                        writeTop10ChangeQuery1(ct.getT1(), ct.getT2(), ct.getT3(), ct.getT4(), ct.getT5());
                    }
                });

        // query 2: Frequent routes
        sharedTripsStream
                .map(t -> {
                    // events leaving window for empty taxis in the last 30 minutes
                    while (tripEmptyTaxis.peek() != null && tripEmptyTaxis.peek().getDropOffTimestamp() <
                            t.getDropOffTimestamp() - 30 * 60 * 1000) {
                        // remove it from queue
                        Trip trip = tripEmptyTaxis.poll();

                        // tell the root Actor to decrement the empty taxis for the end cell in the removed trip
                        inbox.send(rootTop10ProfitableCells, new CellProfitabilityActor.DecrementEmptyTaxis
                                (trip.getRoute250().getEndCell(), trip.getRoute250().getEndCell().getId(),
                                        trip.getId()));

                        // block until we get back the top 10
                        CellProfitabilityActor.Top10EmptyTaxis cellActorTop10 = (CellProfitabilityActor.Top10EmptyTaxis)
                                inbox.receive(Duration.create(10000, "seconds"));
                        List<CellProfitability> newTop10ProfitableCells = Arrays.asList(cellActorTop10.top10ProfitableCells);
                        Collections.sort(newTop10ProfitableCells, Comparator.<CellProfitability>reverseOrder());

                        if (!newTop10ProfitableCells.equals(top10ProfitableCells)) {
                            top10ProfitableCells.clear();
                            top10ProfitableCells.addAll(newTop10ProfitableCells);
                            writeTop10ChangeQuery2(top10ProfitableCells, trip.getPickupDatetime().plusMinutes(30),
                                    trip.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived());
                        }
                    }

                    // events leaving the window for profit cells in the last 15 minutes
                    while (tripProfits.peek() != null && tripProfits.peek().getDropOffTimestamp() <
                            t.getDropOffTimestamp() - 15 * 60 * 1000) {
                        Trip trip = tripProfits.poll();

                        // tell the root Actor to decrement the empty taxis for the end cell in the removed trip
                        inbox.send(rootTop10ProfitableCells, new CellProfitabilityActor.RemoveFareTipAmount(
                                trip.getRoute250().getStartCell(), trip.getRoute250().getStartCell().getId(),
                                        trip.getId(), trip.getFareAmount() + trip.getTipAmount()));

                        // block until we get back the top 10
                        CellProfitabilityActor.Top10FareTipAmount cellActorTop10 = (CellProfitabilityActor.Top10FareTipAmount)
                                inbox.receive(Duration.create(10000, "seconds"));
                        List<CellProfitability> newTop10ProfitableCells = Arrays.asList(cellActorTop10.top10ProfitableCells);
                        Collections.sort(newTop10ProfitableCells, Comparator.<CellProfitability>reverseOrder());

                        if (!newTop10ProfitableCells.equals(top10ProfitableCells)) {
                            top10ProfitableCells.clear();
                            top10ProfitableCells.addAll(newTop10ProfitableCells);
                            writeTop10ChangeQuery2(top10ProfitableCells, trip.getPickupDatetime().plusMinutes(30),
                                    trip.getDropOffDatetime().plusMinutes(30),
                                    t.getTimestampReceived());
                        }
                    }

                    // add to window
                    tripEmptyTaxis.add(t);
                    tripProfits.add(t);

                    // tell the root Actor to increment the empty taxis count for the end cell in the added trip
                    // reply should go to the inbox
                    inbox.send(rootTop10Routes, new CellProfitabilityActor.IncrementEmptyTaxis
                            (t.getRoute250().getEndCell(), t.getRoute250().getEndCell().getId(), t.getId()));
                    inbox.send(rootTop10Routes, new CellProfitabilityActor.AddFareTipAmount
                            (t.getRoute250().getStartCell(), t.getRoute250().getStartCell().getId(), t.getId(),
                                    t.getTipAmount() + t.getFareAmount()));

//                    CellProfitabilityActor.Top10FareTipAmount cellActorTop10_1 = (RouteActor.Top10) inbox.receive(Duration.create(10000, "seconds"));
//                    List<RouteCount> newTop10Routes = Arrays.asList(routeActorTop10.top10Routes);
//                    Collections.sort(newTop10Routes, Comparator.<RouteCount>reverseOrder());

//                    return Tuple.of(newTop10Routes, t.getPickupDatetime(), t.getDropOffDatetime(),
//                            t.getTimestampReceived(), t);
                    return null;
                })
                .observeComplete(v -> {
                    completeSignal.countDown();
                })
                .consume(ct -> {
//                    if (!top10Routes.equals(ct.getT1())) {
//                        top10Routes.clear();
//                        top10Routes.addAll(ct.getT1());
//                        writeTop10ChangeQuery1(ct.getT1(), ct.getT2(), ct.getT3(), ct.getT4(), ct.getT5());
//                    }
                });

        // read the stream from file: for local testing
        taxiStream.readStream();

        // wait for onComplete event
        completeSignal.await();
        id = 0;
        system.shutdown();
        return System.currentTimeMillis() - startTime;
    }
}
