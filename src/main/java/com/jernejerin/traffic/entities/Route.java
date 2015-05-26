package com.jernejerin.traffic.entities;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A route between start cell and end cell.
 *
 * @author Jernej Jerin
 */
public class Route {
    private long id;
    private Cell startCell;
    private Cell endCell;

    public Route(Cell startCell, Cell endCell) {
        this.startCell = startCell;
        this.endCell = endCell;
        // convert from 300-base system to decimal system
        this.id = startCell.getEast() * 300L * 300L * 300L + startCell.getSouth() * 300L * 300L +
                endCell.getEast() * 300L + endCell.getSouth();
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
    public String toString() {
        return "(" + this.startCell + ", " + this.endCell + ")";
    }
}
