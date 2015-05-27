package com.jernejerin.traffic.entities;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.aliasi.util.BoundedPriorityQueue;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Defines Route Actor and the messages that we can pass between actors.
 *
 * @author Jernej Jerin
 */
public class RouteActor {
    // first are defined messages, that are exchanged between actors

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
     * Message to increment the count for given Route.
     */
    public static class IncrementRoute implements Serializable {
        public static final long serialVersionUID = 1;
        public final Route route;
        public final long routeId;
        public final int tripId;

        public IncrementRoute(Route route, long routeId, int tripId) {
            this.route = route;
            this.routeId = routeId;
            this.tripId = tripId;
        }
    }

    /**
     * Message to decrement the count for given Route.
     */
    public static class DecrementRoute implements Serializable {
        public static final long serialVersionUID = 1;
        public final Route route;

        public DecrementRoute(Route route) {
            this.route = route;
        }
    }

    // define Actors

    /**
     * Inner node Actor that contains top routes.
     */
    public static class TopRouteNode extends AbstractActor {
        // ACTOR STATE
        // top routes for a group of actors
        BoundedPriorityQueue<RouteCount> top10Routes = new BoundedPriorityQueue<>
                (Comparator.<RouteCount>naturalOrder(), 10);

        // Actor references to the next 10 child Actors
        ActorRef[] topRouteNodes = new ActorRef[10];

        ActorRef parentNode;

        // message count of updates that we get from child actors
        int msgCount = 0;

        public TopRouteNode() {
            // define BEHAVIOUR on message receive
            receive(ReceiveBuilder.
                    // message that contains request from parent to get current top 10 routes. Respond with current
                    // top 10 routes.
                    match(GetTop10.class, message -> sender().tell(new Top10((RouteCount[]) top10Routes.toArray()), self())).

                    // increment route message
                    match(IncrementRoute.class, message -> {
                        parentNode = sender();
                        msgCount = 0;
                        // clear top 10 routes
                        top10Routes.clear();

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
                                        String.valueOf(id));
                            } else {
                                // otherwise create normal node
                                topRouteNodes[bucket] = context().actorOf(Props.create(TopRouteNode.class),
                                        String.valueOf(id));
                            }
                        }

                        // send the message to the actor, that is contained in that bucket
                        topRouteNodes[bucket].tell(new IncrementRoute(message.route, id, message.tripId), self());
                        msgCount++;

                        // send message to all other actors to get top routes
                        for (int i = 0; i < topRouteNodes.length; i++) {
                            if (i != bucket && topRouteNodes[i] != null) {
                                topRouteNodes[i].tell(new GetTop10(), self());
                                msgCount++;
                            }
                        }
                    }).
//                    match(DecrementRoute.class, message -> )

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
                            msgCount = 0;
                            // send these top 10 to parent
                            parentNode.tell(new Top10(top10Routes.toArray(new RouteCount[10])), self());
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
                    match(IncrementRoute.class, message -> {
                        // if it does not exist yet, add a new route count with count set to 1
                        if (routesCount[(int)message.routeId] == null) {
                            routesCount[(int)message.routeId] = new RouteCount(message.route, message.tripId, 1);
                        } else {
                            // otherwise update count and id to the trip id
                            routesCount[(int)message.routeId].setCount(routesCount[(int)message.routeId].getCount() + 1);
                            routesCount[(int)message.routeId].setId(message.tripId);
                        }

                        // return this routes to the parent node
                        sender().tell(new Top10(routesCount), self());
                    }).
                    build());
        }
    }
}
