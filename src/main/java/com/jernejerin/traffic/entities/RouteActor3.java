package com.jernejerin.traffic.entities;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.aliasi.util.BoundedPriorityQueue;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;

/**
 * Defines Route Actor and the messages that we can pass between actors.
 * An example of a bottom up approach.
 *
 * @author Jernej Jerin
 */
public class RouteActor3 {
    // first are defined messages, that are exchanged between actors
    // All messages are immutable as to avoid the shared mutable state trap

    /**
     * Message for getting the top 10 routes from the Actor.
     */
    public static class GetTop10 implements Serializable {
        public static final long serialVersionUID = 1;
    }

    /**
     * Message for returning the top 10 routes from the Actor.
     */
    public static class Top10 implements Serializable {
        public static final long serialVersionUID = 1;
        public final RouteCount[] top10Routes;

        public Top10(RouteCount[] top10Routes) {
            this.top10Routes = top10Routes;
        }
    }

    /**
     * Message for returning the first top 10 routes from the Actor.
     */
    public static class FirstTop10 extends Top10 implements Serializable {
        public static final long serialVersionUID = 1;
        public final long routeId;
        public final ActorRef replyActor;

        public FirstTop10(RouteCount[] top10Routes, long routeId, ActorRef replyActor) {
            super(top10Routes);
            this.routeId = routeId;
            this.replyActor = replyActor;
        }
    }

    /**
     * Message to increment the count for given Route.
     */
    public static class IncrementRoute implements Serializable {
        public static final long serialVersionUID = 1;
        public final Route route;
        public final long routeId;
        public final int tripId;
        public final ActorRef replyActor;

        public IncrementRoute(Route route, long routeId, int tripId, ActorRef replyActor) {
            this.route = route;
            this.routeId = routeId;
            this.tripId = tripId;
            this.replyActor = replyActor;
        }
    }

    /**
     * Message to decrement the count for given Route.
     */
    public static class DecrementRoute implements Serializable {
        public static final long serialVersionUID = 1;
        public final Route route;
        public final long routeId;

        public DecrementRoute(Route route, long routeId) {
            this.route = route;
            this.routeId = routeId;
        }
    }

    // define Actors

    /**
     * Inner node Actor that contains top route count.
     */
    public static class TopRouteNode extends AbstractActor {
        // ACTOR STATE
        // top routes for a group of actors
        BoundedPriorityQueue<RouteCount> top10Routes = new BoundedPriorityQueue<>
                (Comparator.<RouteCount>naturalOrder(), 10);

        // Actor references to the next 10 child Actors
        ActorRef[] topRouteNodes = new ActorRef[10];
        ActorRef replyActor;
        ActorRef parentNode;
        long id;

        // message count of updates that we get from child actors
        int msgCount = 0;

        public TopRouteNode() {
            // define BEHAVIOUR on message receive
            receive(ReceiveBuilder.
                    // message that contains request from parent to get current top 10 routes. Respond with current
                    // top 10 routes.
                    match(GetTop10.class, message -> {
                        sender().tell(new Top10(top10Routes.toArray(new RouteCount[top10Routes.size()])), self());
                    }).

                    // increment route message
                    match(FirstTop10.class, message -> {
                        // check if parent node exists
                        if (parentNode == null) {
                            replyActor = message.replyActor;
                            if (message.routeId == 0) {
                                // id is 0, this is the root node. For the parent node set the reply node
                                parentNode = replyActor;
                            } else {
                                // otherwise create normal node
//                                context().ac
                                parentNode = context().actorOf(Props.create(TopRouteNode.class),
                                        String.valueOf(message.routeId / 10));
                            }

                            // id of the actor
                            id = message.routeId;
                        }
                        msgCount = 0;
                        // clear top 10 routes
                        top10Routes.clear();
                        for (RouteCount routeCount : message.top10Routes) {
                            if (routeCount != null)
                                top10Routes.offer(routeCount);
                        }

                        // compute the bucket location for the selected Actor
                        int bucket = (int) (message.routeId % 10);

                        // check if Actor ref already exists for the computed bucket
                        if (topRouteNodes[bucket] == null) {
                            // does not exist, set the sender as the node
                            topRouteNodes[bucket] = sender();
                        }

                        // send message to all other child actors to get top routes
                        for (int i = 0; i < topRouteNodes.length; i++) {
                            if (i != bucket && topRouteNodes[i] != null) {
                                topRouteNodes[i].tell(new GetTop10(), self());
                                msgCount++;
                            }
                        }

                        if (msgCount == 0)
                            parentNode.tell(new FirstTop10(top10Routes.toArray(new RouteCount[top10Routes.size()]),
                                    id / 10, replyActor), self());
                    }).

                    // receive message from child which is returning its top 10 routes
                    match(Top10.class, message -> {
                        msgCount--;

                        // add to top 10 routes
                        for (RouteCount routeCount : message.top10Routes) {
                            if (routeCount != null)
                                top10Routes.offer(routeCount);
                        }

                        // all updates from child actors have arrived
                        if (msgCount == 0) {
                            // send these top 10 to parent
                            parentNode.tell(new FirstTop10(top10Routes.toArray(new RouteCount[top10Routes.size()]),
                                id / 10, replyActor), self());
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
        ActorRef parentNode;

        public TopRouteLeafNode() {
            receive(ReceiveBuilder.
                    // message to increment route
                    match(IncrementRoute.class, message -> {
                        // if it does not exist yet, add a new route count with count set to 1
                        if (routesCount[(int) (message.routeId % 10)] == null) {
                            routesCount[(int) (message.routeId % 10)] = new RouteCount(message.route, message.tripId, 1);
                        } else {
                            // otherwise update count and id to the trip id
                            routesCount[(int) (message.routeId % 10)].setCount(routesCount[(int) (message.routeId % 10)].getCount() + 1);
                            routesCount[(int) (message.routeId % 10)].setId(message.tripId);
                        }

                        // check if parent node exists
                        if (parentNode == null) {
                            parentNode = context().actorOf(Props.create(TopRouteNode.class),
                                    String.valueOf(message.routeId / 100));
                        }

                        // return this routes to the parent node
                        parentNode.tell(new FirstTop10(routesCount, message.routeId / 100, sender()), self());
                    }).

                    // message to decrement route
                    match(DecrementRoute.class, message -> {
                        // update count
                        routesCount[(int) (message.routeId % 10)].setCount(routesCount[(int) message.routeId].getCount() - 1);

                        // return this routes to the parent node
                        parentNode.tell(new FirstTop10(routesCount, message.routeId / 100, sender()), self());
                    }).

                    // message to return top 10 nodes from leaf
                    match(Top10.class, message -> {
                        parentNode.tell(new Top10(routesCount), self());
                    }).
                    build());
        }
    }
}
