package com.jernejerin.traffic.entities;

import com.jernejerin.traffic.helper.MedianOfStream;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A value class that holds count of Routes and the logic for combining.
 *
 * <b>
 *     Note: this class has a natural ordering that is inconsistent with equals.
 *
 * @author Jernej Jerin
 */
public class RouteCount implements Comparable<RouteCount> {
    private final Route route;
    private final int id;
    private final long count;

    public RouteCount(Route route, int id, long count) {
        this.route = route;
        this.id = id;
        this.count = count;
    }

    public Route getRoute() {
        return route;
    }

    public int getId() {
        return id;
    }

    public long getCount() {
        return count;
    }

    public static RouteCount fromTrip(Trip trip) {
        return new RouteCount(trip.getRoute500(), trip.getId(), 1L);
    }

    public static RouteCount combine(RouteCount routeCount1, RouteCount routeCount2) {
        RouteCount recent = routeCount1.id > routeCount2.id ? routeCount1 : routeCount2;
        return new RouteCount(recent.route, recent.id, routeCount1.count + routeCount2.count);
    }

    @Override
    /**
     * Compute hash code by using Apache Commons Lang HashCodeBuilder.
     */
    public int hashCode() {
        return new HashCodeBuilder(73, 79)
                .append(this.route)
                .toHashCode();
    }

    @Override
    /**
     * Compute equals by using Apache Commons Lang EqualsBuilder.
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof RouteCount))
            return false;
        if (obj == this)
            return true;

        RouteCount routeCount = (RouteCount) obj;
        return new EqualsBuilder()
                .append(this.route, routeCount.route)
                .isEquals();
    }

    @Override
    public int compareTo(RouteCount routeCount) {
        /* Question 7: How should we order elements in a list that have the same value for the ordering criterion?
            Answer: You should always put the freshest information first. E.g. if route A and B have the same
            frequency, put the route with the freshest input information fist (i.e. the one which includes
            the freshest event).*/
        if (this.count < routeCount.count)
            return -1;
        else if (this.count > routeCount.count)
            return 1;
        else {
            // if contains drop off timestamps, order by last timestamp in drop off
            // the highest timestamp has preceding
            if (this.id < routeCount.id)
                return -1;
            else if (this.id > routeCount.id)
                return 1;
            else
                return 0;
        }
    }
}
