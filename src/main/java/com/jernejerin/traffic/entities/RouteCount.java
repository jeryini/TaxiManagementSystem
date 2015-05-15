package com.jernejerin.traffic.entities;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created by Jernej on 14.5.2015.
 */
public class RouteCount implements Comparable<RouteCount> {
    public final Route route;
    public long count;

    public RouteCount(Route r, long c) {
        this.route = r;
        this.count = c;
    }

    public static RouteCount fromRoute(Route r) {
        return new RouteCount(r, 1L);
    }

    public static RouteCount combine(RouteCount rc1, RouteCount rc2) {
        Route recent;
        if (rc1.route.getLastUpdated() > rc2.route.getLastUpdated()) {
            recent = rc1.route;
        } else {
            recent = rc2.route;
        }
        return new RouteCount(recent, rc1.count + rc2.count);
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
            if (this.route.getLastUpdated() < routeCount.route.getLastUpdated())
                return -1;
            else if (this.route.getLastUpdated() > route.getLastUpdated())
                return 1;
            else
                return 0;
        }
    }
}
