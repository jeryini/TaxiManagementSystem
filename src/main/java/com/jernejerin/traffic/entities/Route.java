package com.jernejerin.traffic.entities;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <p>
 * A route between start cell and end cell. It stores the list of all the current trip timestamps for the given
 * window.
 *
 * <b>Note: this class has a natural ordering that is inconsistent with equals.</b>
 * </p>
 *
 * @author Jernej Jerin
 */
public class Route implements Comparable<Route> {
    private long lastUpdated;
    private Deque<Long> dropOffWindow;
    private Deque<Long> pickUpWindow;
    private int dropOffSize;
    private Cell startCell;
    private Cell endCell;
    private long dropOff;
    private LocalDateTime pickupDatetime; // time when the passenger(s) were picked up
    private LocalDateTime dropOffDatetime; // time when the passenger(s) were dropped off

    public Route(Cell startCell, Cell endCell, long lastUpdated, LocalDateTime pickupDatetime,
                 LocalDateTime dropOffDatetime) {
        this.lastUpdated = lastUpdated;
        this.startCell = startCell;
        this.endCell = endCell;
        this.dropOffWindow = new ArrayDeque<>();
        this.pickUpWindow = new ArrayDeque<>();
        this.dropOffSize = 0;
        this.pickupDatetime = pickupDatetime;
        this.dropOffDatetime = dropOffDatetime;
    }

    public long getLastUpdated() {
        return this.lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public Cell getStartCell() {
        return startCell;
    }

    public void setStartCell(Cell startCell) {
        this.startCell = startCell;
    }

    public Cell getEndCell() {
        return endCell;
    }

    public void setEndCell(Cell endCell) {
        this.endCell = endCell;
    }

    public Queue<Long> getDropOffWindow() {
        return dropOffWindow;
    }

    public void setDropOffWindow(ArrayDeque<Long> dropOffWindow) {
        this.dropOffWindow = dropOffWindow;
    }

    public Queue<Long> getPickUpWindow() {
        return pickUpWindow;
    }

    public void setPickUpWindow(ArrayDeque<Long> pickUpWindow) {
        this.pickUpWindow = pickUpWindow;
    }

    public void setDropOffSize(int dropOffSize) {
        this.dropOffSize = dropOffSize;
    }

    public int getDropOffSize() {
        return this.dropOffSize;
    }

    public long getDropOff() {
        return dropOff;
    }

    public void setDropOff(long dropOff) {
        this.dropOff = dropOff;
    }

    public LocalDateTime getPickupDatetime() {
        return pickupDatetime;
    }

    public void setPickupDatetime(LocalDateTime pickupDatetime) {
        this.pickupDatetime = pickupDatetime;
    }

    public LocalDateTime getDropOffDatetime() {
        return dropOffDatetime;
    }

    public void setDropOffDatetime(LocalDateTime dropOffDatetime) {
        this.dropOffDatetime = dropOffDatetime;
    }

    @Override
    /**
     * Compute hash code by using Apache Commons Lang HashCodeBuilder.
     */
    public int hashCode() {
        return new HashCodeBuilder(43, 59)
                .append(this.startCell)
                .append(this.endCell)
                .toHashCode();
    }

    @Override
    /**
     * Compute equals by using Apache Commons Lang EqualsBuilder.
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof Route))
            return false;
        if (obj == this)
            return true;

        Route route = (Route) obj;
        return new EqualsBuilder()
                .append(this.startCell, route.startCell)
                .append(this.endCell, route.endCell)
                .isEquals();
    }

    @Override
    public int compareTo(Route route) {
        /* Question 7: How should we order elements in a list that have the same value for the ordering criterion?
            Answer: You should always put the freshest information first. E.g. if route A and B have the same
            frequency, put the route with the freshest input information fist (i.e. the one which includes
            the freshest event).*/
        if (this.dropOffSize < route.dropOffSize)
            return -1;
        else if (this.dropOffSize > route.dropOffSize)
            return 1;
        else {
                // if contains drop off timestamps, order by last timestamp in drop off
                // the highest timestamp has preceding
                // as we are comparing nanoseconds we should use {@code t1 - t0 < 0}, not {@code t1 < t0},
                // because of the possibility of numerical overflow.
            if (this.lastUpdated -  route.lastUpdated < 0)
                return -1;
            else if (this.lastUpdated - route.lastUpdated > 0)
                return 1;
            else
                return 0;
        }
    }
}
