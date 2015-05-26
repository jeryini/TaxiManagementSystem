package com.jernejerin.traffic.entities;

import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;

import java.io.Serializable;
import java.util.List;


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
        public final List<RouteCount> top10Routes;

        public Top10(List<RouteCount> top10Routes) {
            this.top10Routes = top10Routes;
        }
    }

    /**
     * Message to increment the count for given Route.
     */
    public static class IncrementRoute implements Serializable {
        public static final long serialVersionUID = 1;
        public final Route route;

        public IncrementRoute(Route route) {
            this.route = route;
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
     * Actor for inner node that contains top routes.
     */
    public static class TopRouteNode extends AbstractActor {
        List<RouteCount> top10Routes;

        public TopRouteNode() {
            // define behaviour on message receive
            receive(ReceiveBuilder.
                    // message that contains request to get current top 10 routes
                    match(GetTop10.class, message -> sender().tell(new Top10(top10Routes), self())).
                    build());
        }
    }
}
