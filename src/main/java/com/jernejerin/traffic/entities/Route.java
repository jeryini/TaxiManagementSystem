package com.jernejerin.traffic.entities;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * <p>
 * A route between start cell and end cell. It stores the list of all the current trip timestamps for the given
 * window.
 * </p>
 *
 * @author Jernej Jerin
 */
public class Route implements Comparable<Route> {
    private Queue<Long> dropOff;
    private Cell startCell;
    private Cell endCell;

    public Route(Cell startCell, Cell endCell) {
        this.startCell = startCell;
        this.endCell = endCell;
        this.dropOff = new LinkedList<>();
    }

    public Queue<Long> getDropOff() {
        return dropOff;
    }

    public void setDropOff(Queue<Long> dropOff) {
        this.dropOff = dropOff;
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
        return (this.dropOff.size() < route.dropOff.size()) ? -1: (this.dropOff.size() > route.dropOff.size()) ? 1 : 0;
    }
}
