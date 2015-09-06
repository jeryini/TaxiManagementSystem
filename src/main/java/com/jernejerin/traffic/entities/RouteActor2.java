package com.jernejerin.traffic.entities;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.RepointableActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.aliasi.util.BoundedPriorityQueue;
import com.jernejerin.traffic.architectures.Architecture;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Defines Route Actor and the messages that we can pass between actors.
 * The logic implements the following postulates:
 * 1. Receive message GETTOP10:
 * 1.1 If node locked, save into queue for further processing.
 * 1.2 If node is processing INCREMENTROUTE/DECREMENTROUTE then lock the node and save message into queue for further processing.
 * 1.3 Otherwise return current TOP10 nodes.
 *
 * 2. Receive message INCREMENTROUTE:
 * 2.1 If node locked, save into queue for further processing.
 * 2.2 Otherwise send message GETTOP10 to 9 children and message INCREMENTROUTE to 1 child. Can we send GETTOP request even though locked?
 *
 * 3. Receive message DECREMENTROUTE:
 * 3.1 If node locked, save into queue for further processing.
 * 3.2 Otherwise send message GETTOP10 to 9 children and message DECREMENTROUTE to 1 child.
 *
 * 4. Receive message TOP10:
 * 4.1 Insert returned nodes into priority queue.
 * 4.2 If all 10 TOP10 messages have arrived, then send TOP10 message to parent.
 *
 * @author Jernej Jerin
 */
public class RouteActor2 {
    // first are defined messages, that are exchanged between actors
    // All messages are immutable as to avoid the shared mutable state trap

    /**
     * Message for initializing the node for writing the changes to output.
     */
    public static class InitializeTopRouteWriteChangesNode implements Serializable {
        public static final long serialVersionUID = 1;
        public final Architecture architecture;

        public InitializeTopRouteWriteChangesNode(Architecture architecture) {
            this.architecture = architecture;
        }
    }

    /**
     * Message for getting the top 10 routes from the Actor.
     */
    public static class GetTop10 implements Serializable {
        public static final long serialVersionUID = 1;

        public final Trip trip;
        public final long timestampReceived;
        public final int numEvents;

        public GetTop10(Trip trip, int numEvents, long timestampReceived) {
            this.trip = trip;
            this.timestampReceived = timestampReceived;
            this.numEvents = numEvents;
        }
    }

    /**
     * Message for returning the top 10 routes from the Actor.
     */
    public static class Top10 implements Serializable {
        public static final long serialVersionUID = 1;
        public final RouteCount[] top10Routes;
        public final Trip trip;
        public final long timestampReceived;
        public final int numEvents;

        public Top10(RouteCount[] top10Routes, Trip trip, long timestampReceived, int numEvents) {
            this.top10Routes = top10Routes;
            this.trip = trip;
            this.timestampReceived = timestampReceived;
            this.numEvents = numEvents;
        }
    }

    /**
     * Message to increment the count for given Route.
     */
    public static class IncrementRoute implements Serializable {
        public static final long serialVersionUID = 1;
        public final Trip trip;
        public final long routeId;
        public final long timestampReceived;
        public final int numEvents;

        public IncrementRoute(Trip trip, long routeId, long timestampReceived, int numEvents) {
            this.trip = trip;
            this.routeId = routeId;
            this.timestampReceived = timestampReceived;
            this.numEvents = numEvents;
        }
    }

    /**
     * Message to decrement the count for given Route.
     */
    public static class DecrementRoute implements Serializable {
        public static final long serialVersionUID = 1;
        public final Trip trip;
        public final long routeId;
        public final long timestampReceived;
        public final int numEvents;

        public DecrementRoute(Trip trip, long routeId, long timestampReceived, int numEvents) {
            this.trip = trip;
            this.routeId = routeId;
            this.timestampReceived = timestampReceived;
            this.numEvents = numEvents;
        }
    }

    /**
     * Message for completed event.
     */
    public static class CompletedEvent implements Serializable {
        public static final long serialVersionUID = 1;
        public final int numEvents;

        public CompletedEvent(int numEvents) {
            this.numEvents = numEvents;
        }
    }

    // define Actors

    /**
     * Inner node Actor that contains top route count.
     */
    public static class TopRouteNode extends AbstractActor {
        // ACTOR STATE
        // top routes for a group of actors. This is to support receiving multiple increment/decrement messages without
        // finishing processing the first message
        HashMap<Integer, BoundedPriorityQueue<RouteCount>>  top10Routes = new HashMap<>();

        // message count of updates that we get from child actors. This also si
        HashMap<Integer, Integer> msgCount = new HashMap<>();

        // contains top10 after all the children return the top10 routes
        BoundedPriorityQueue<RouteCount> top10RoutesCurrent = new BoundedPriorityQueue<>
                (Comparator.<RouteCount>naturalOrder(), 10);

        // Actor references to the next 10 child Actors
        ActorRef[] topRouteNodes = new ActorRef[10];
        ActorRef parentNode;

        // messages stored for processing, we need FIFO order
        ArrayDeque<Object> messages = new ArrayDeque<>();


        public TopRouteNode() {
            // define BEHAVIOUR on message receive
            receive(ReceiveBuilder.
                    // message contains initialization values for actor that writes top 10 changes to file
                    // this message is sent ONLY ONCE to the root node
                    // THIS MESSAGE COMES FROM OUTSIDE AND ONLY TO THE ROOT NODE
                    match(InitializeTopRouteWriteChangesNode.class, message -> {
                        // parent node of the root node shall point to the actor that writes changes to file
                        parentNode = context().actorOf(Props.create(TopRouteWriteChangesNode.class),
                                "TopRouteWriteChangesNode");

                        // send message to the node to initialize its values
                        parentNode.tell(message, self());
                    }).

                    // receive message from outside for completed event
                    // THIS MESSAGE COMES FROM OUTSIDE
                    match(CompletedEvent.class, message -> {
                        // send query to the parent node with sender set to outside actor
                        parentNode.tell(new CompletedEvent(message.numEvents), sender());
                    }).

                    // message that contains request from parent to get current top 10 routes. Respond with current
                    // top 10 routes. THIS MESSAGE COMES FROM PARENT OR ITSELF
                    match(GetTop10.class, message -> {
                        // if IncrementRoute/DecrementRoute is in progress then we need to store any future
                        // GetTop10 messages to queue
                        if (!msgCount.isEmpty()) {
                            messages.add(message);
                        } else {
                            // now we can return Top10 as there are no active IncrementRoute/DecrementRoute
                            // processing going on
                            sender().tell(new Top10(top10RoutesCurrent.toArray(new RouteCount[top10RoutesCurrent.size()]),
                                    message.trip, message.timestampReceived, message.numEvents), self());
                        }
                    }).

                    // increment route message
                    // THIS MESSAGE COMES FROM PARENT OR OUTSIDE
                    match(IncrementRoute.class, message -> {
                        if (!messages.isEmpty()) {
                            messages.add(message);
                        } else {
//                            numEvents++;
                            // store parent node so we can send messages back
                            if (parentNode == null) {
                                parentNode = sender();
                            }

                            // store message count and top10 routes to key value, where key represents trip with unique
                            // event number
                            msgCount.put(message.numEvents, 0);
                            top10Routes.put(message.numEvents, new BoundedPriorityQueue<>(Comparator.<RouteCount>naturalOrder(), 10));

                            // compute the bucket location for the selected Actor
                            int bucket = (int) (message.routeId % 10);

                            // and the new id
                            long id = message.routeId / 10;

                            // check if Actor ref already exists for the computed bucket
                            if (topRouteNodes[bucket] == null) {
                                // does not exist, create it
                                // the type of node depends on the value of id compare to 10
                                if (id < 10) {
                                    // id less then 10, then create leaf node
                                    topRouteNodes[bucket] = context().actorOf(Props.create(TopRouteLeafNode.class),
                                            String.valueOf(bucket));
                                } else {
                                    // otherwise create normal node
                                    topRouteNodes[bucket] = context().actorOf(Props.create(TopRouteNode.class),
                                            String.valueOf(bucket));
                                }
                            }

                            // send the message to the actor, that is contained in that bucket
                            topRouteNodes[bucket].tell(new IncrementRoute(message.trip, id, message.timestampReceived,
                                    message.numEvents), self());
                            msgCount.put(message.numEvents, 1);

                            // send message to all other actors to get top routes
                            for (int i = 0; i < topRouteNodes.length; i++) {
                                if (i != bucket && topRouteNodes[i] != null) {
                                    topRouteNodes[i].tell(new GetTop10(message.trip, message.numEvents,
                                            message.timestampReceived), self());
                                    // increment the number of sent GetTop10 messages for the given event id
                                    msgCount.put(message.numEvents, msgCount.get(message.numEvents) + 1);
                                }
                            }
                        }
                    }).

                    // receive message for decrementing route count value
                    // THIS MESSAGE COMES FROM PARENT
                    match(DecrementRoute.class, message -> {
                        if (!messages.isEmpty()) {
                            messages.add(message);
                        } else {
//                            numEvents++;
                            if (parentNode == null) {
                                parentNode = sender();
                            }

                            msgCount.put(message.numEvents, 0);
                            top10Routes.put(message.numEvents, new BoundedPriorityQueue<>(Comparator.<RouteCount>naturalOrder(), 10));

                            // compute the bucket location for the selected Actor
                            int bucket = (int) (message.routeId % 10);

                            // and the new id
                            long id = message.routeId / 10;

                            // send the message to the actor, that is contained in that bucket
                            // these top route nodes should always exist as the IncrementRoute message must
                            // have been there before them
                            topRouteNodes[bucket].tell(new DecrementRoute(message.trip, id, message.timestampReceived,
                                    message.numEvents), self());
                            msgCount.put(message.numEvents, 1);

                            // send message to all other actors to get top routes
                            for (int i = 0; i < topRouteNodes.length; i++) {
                                if (i != bucket && topRouteNodes[i] != null) {
                                    topRouteNodes[i].tell(new GetTop10(message.trip, message.numEvents,
                                            message.timestampReceived), self());
                                    msgCount.put(message.numEvents, msgCount.get(message.numEvents) + 1);
                                }
                            }
                        }
                    }).

                    // receive message from child which is returning its top 10 routes
                    // THIS MESSAGE COMES FROM CHILD
                    match(Top10.class, message -> {
                        // decrease by one the number of GetTop10 messages that were sent from this actor
                        msgCount.put(message.numEvents, msgCount.get(message.numEvents) - 1);

                        // add to top 10 routes
                        BoundedPriorityQueue<RouteCount> top10RoutesEvent = top10Routes.get(message.numEvents);
                        for (RouteCount routeCount : message.top10Routes) {
                            if (routeCount != null)
                                top10RoutesEvent.offer(routeCount);
                        }

                        // all updates from child actors for this trip event have arrived
                        if (msgCount.get(message.numEvents) == 0) {
                            msgCount.remove(message.numEvents);
                            top10Routes.remove(message.numEvents);

                            // set the current top10
                            top10RoutesCurrent.clear();
                            top10RoutesEvent.forEach(route -> top10RoutesCurrent.offer(route));

                            // send these top 10 to parent
                            parentNode.tell(new Top10(top10RoutesCurrent.toArray(new RouteCount[top10RoutesCurrent.size()]),
                                    message.trip, message.timestampReceived, message.numEvents), self());

                            // resend messages that had to wait in queue for the current operation to finish
                            if (msgCount.isEmpty()) {
                                while (messages.peek() != null)
                                    self().tell(messages.pop(), parentNode);
                            }
                        }
                    }).
                    build());
        }
    }

    /**
     * Leaf Actor that contains actual Route Count.
     */
    public static class TopRouteLeafNode extends AbstractActor {
        // routes that fall in this leaf node
        RouteCount[] routesCount = new RouteCount[10];

        public TopRouteLeafNode() {
            receive(ReceiveBuilder.
                    // message to increment route
                    match(IncrementRoute.class, message -> {
                        // if it does not exist yet, add a new route count with count set to 1
                        if (routesCount[(int)message.routeId] == null) {
                            routesCount[(int)message.routeId] = new RouteCount(message.trip.getRoute500(), message.trip.getId(), 1);
                        } else {
                            // otherwise update count and id to the trip id
                            routesCount[(int)message.routeId].setCount(routesCount[(int)message.routeId].getCount() + 1);
                            routesCount[(int)message.routeId].setId(message.trip.getId());
                        }

                        // return this routes to the parent node
                        sender().tell(new Top10(routesCount, message.trip, message.timestampReceived,
                                message.numEvents), self());
                    }).

                    // message to decrement route
                    match(DecrementRoute.class, message -> {
                        // update count
                        routesCount[(int) message.routeId].setCount(routesCount[(int) message.routeId].getCount() - 1);

                        // return this routes to the parent node
                        sender().tell(new Top10(routesCount, message.trip, message.timestampReceived, message.numEvents), self());
                    }).
                    build());
        }
    }

    /**
     * Write Actor that writes changes in top 10 to file.
     */
    public static class TopRouteWriteChangesNode extends AbstractActor {
        private Architecture architecture;
        // number of processed events
        int numEvents = 0;
        private List<RouteCount> top10Routes = new LinkedList<>();

        public TopRouteWriteChangesNode() {
            receive(ReceiveBuilder.
                // initialization message to set an instance to the architecture
                match(InitializeTopRouteWriteChangesNode.class, message -> {
                    architecture = message.architecture;
                }).
                // message to write changes in the top 10 file
                match(Top10.class, message -> {
                    this.numEvents++;
                    List<RouteCount> newTop10Routes = Arrays.asList(message.top10Routes);
                    Collections.sort(newTop10Routes, Comparator.<RouteCount>reverseOrder());
                    if (!newTop10Routes.equals(top10Routes)) {
                        top10Routes.clear();
                        top10Routes.addAll(newTop10Routes);
                        architecture.writeTop10ChangeQuery1(newTop10Routes, message.trip.getPickupDatetime(),
                                message.trip.getDropOffDatetime(), message.timestampReceived, message.trip);
                    }
                }).
                // message to check number of processed trip events
                match(CompletedEvent.class, message -> {
                    // send back the event with the current count of processed events
                    sender().tell(new CompletedEvent(this.numEvents), self());
                }).
                build());
        }
    }
}
