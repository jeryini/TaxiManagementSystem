package com.jernejerin.traffic.architectures;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import com.jernejerin.traffic.client.TaxiStream;
import com.jernejerin.traffic.entities.*;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.fn.tuple.Tuple;
import reactor.rx.Stream;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * An example of a bottom up approach Actor based event driven architecture - AEDA.
 *
 * @author Jernej Jerin
 */
public class AEDA3 extends Architecture {
    private final static Logger LOGGER = Logger.getLogger(AEDA2.class.getName());
    private static int id = 0;
    private static int numEvents = 0;

    public AEDA3(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting single threaded AEDA 3 solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder().fileNameQuery1Output("output/query/" +
                AEDA3.class.getSimpleName() + "_query1.txt").fileNameQuery2Output("output/query/" +
                AEDA3.class.getSimpleName() + "_query2.txt").fileNameInput("trips_10_trips.csv");

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the AEDA solution using the builder
        Architecture aeda3 = new AEDA3(builder);

        // run the solution
        aeda3.run();
    }

    public long run() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // create a taxi service
        this.taxiStream = new TaxiStream("/com/jernejerin/" + this.fileNameInput);

        // Actor factory for creating Actors.
        // only one per application
        final ActorSystem system = ActorSystem.create("actorFactory");

        // create an "actor-in-a-box". It contains an Actor which can be used as a puppet for sending messages
        // to other Actors and receiving their replies.
        final Inbox inbox = Inbox.create(system);
        final Inbox inboxProfitableCells = Inbox.create(system);

        // Create top level actor, supervised by the actor system's provided guardian actor.
        // This will be our root Actor for query 1.
        final ActorRef rootTop10Routes = system.actorOf(Props.create(RouteActor2.TopRouteNode.class), "rootTop10Routes");
        inbox.send(rootTop10Routes, new RouteActor2.InitializeTopRouteWriteChangesNode(this));

        // root Actor for query 2
        final ActorRef rootTop10ProfitableCells = system.actorOf(Props.create(CellProfitabilityActor.TopCellNode.class), "rootTop10ProfitableCells");

        // map from leaf node id to actor
        HashMap<Long, ActorRef> leafNodes = new HashMap<>(100000);

        // time window for query 1
        final ArrayDeque<Trip> trips = new ArrayDeque<>();

        // time window for query 2
        ArrayDeque<Trip> tripProfits = new ArrayDeque<>();
        ArrayDeque<Trip> tripEmptyTaxis = new ArrayDeque<>();

        // top routes for query 1
        final List<RouteCount> top10Routes = new LinkedList<>();

        // top routes for query 2
        final List<CellProfitability> top10ProfitableCells = new LinkedList<>();

        CountDownLatch completeSignal = new CountDownLatch(1);

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
                .observeComplete(v -> {
                    int numEventsRoot;
                    do {
                        // send completed message until the route actor returns correct number of processed events
                        inbox.send(rootTop10Routes, new RouteActor2.CompletedEvent(numEvents));
                        numEventsRoot = ((RouteActor2.CompletedEvent) inbox.receive(Duration.create(180, "seconds"))).numEvents;

                    } while (numEvents != numEventsRoot);

                    // now we can signal shutdown
                    completeSignal.countDown();
                })
                .map(t -> {
                    // trips leaving the window
                    while (trips.peek() != null && trips.peek().getDropOffTimestamp() < t.getDropOffTimestamp()
                            - 30 * 60 * 1000) {
                        // remove it from queue
                        Trip trip = trips.poll();
                        trip.setPickupDatetime(trip.getPickupDatetime().plusMinutes(30));
                        trip.setDropOffDatetime(trip.getDropOffDatetime().plusMinutes(30));

                        // tell the root Actor to decrement the route count for the route in the removed trip
                        inbox.send(leafNodes.get(trip.getRoute500().getId()), new RouteActor3.DecrementRoute(trip.getRoute500(), trip.getRoute500().getId()));
                        numEvents++;

                        // block until we get back the top 10
                        RouteActor3.FirstTop10 routeActorTop10 = (RouteActor3.FirstTop10) inbox.receive(Duration.create(10000, "seconds"));
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

                    return t;
                })
                .consume(t -> {
                    // add to window
                    trips.add(t);
                    ActorRef leafNode = leafNodes.getOrDefault(t.getRoute500().getId() / 10, null);

                    // check if leaf actor already exists
                    if (leafNode == null) {
                        leafNode = system.actorOf(Props.create(RouteActor3.TopRouteLeafNode.class),
                                String.valueOf(t.getRoute500().getId() / 10));
                        leafNodes.put(t.getRoute500().getId() / 10, leafNode);
                    }

                    // tell the leaf node Actor to increment the route count for the route in the added trip
                    // reply should go to the inbox
                    inbox.send(leafNode, new RouteActor3.IncrementRoute(t.getRoute500(), t.getRoute500().getId(), numEvents, null));
                    RouteActor3.FirstTop10 routeActorTop10 = (RouteActor3.FirstTop10) inbox.receive(Duration.create(10000, "seconds"));
                    List<RouteCount> newTop10Routes = Arrays.asList(routeActorTop10.top10Routes);
                    Collections.sort(newTop10Routes, Comparator.<RouteCount>reverseOrder());

                    if (!newTop10Routes.equals(top10Routes)) {
                        top10Routes.clear();
                        top10Routes.addAll(newTop10Routes);
                        writeTop10ChangeQuery1(top10Routes, t.getPickupDatetime(),
                                t.getDropOffDatetime(),
                                t.getTimestampReceived(), t);
                    }

                    numEvents++;
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
