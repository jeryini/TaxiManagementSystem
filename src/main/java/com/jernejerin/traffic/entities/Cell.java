package com.jernejerin.traffic.entities;

import com.jernejerin.traffic.helper.MedianOfStream;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

import java.util.*;

/**
 * <p>
 * Represents a square of 500m X 500m in a grid. The cell
 * grid starts with cell 0.0, located at 41.474937, -74.913585
 * (in Barryville). The coordinate 41.474937, -74.913585 marks
 * the center of the first cell. Cell numbers increase towards
 * the east and south, with the shift to east being the first
 * and the shift to south the second component of the cell,
 * i.e., cell 3.7 is 3 cells east and 7 cells south of cell 0.0.
 * The overall grid expands 150km south and 150km east from cell
 * 0.0 with the cell 299.299 being the last cell in the grid.
 * </p>
 *
 * @author Jernej Jerin
 */
public class Cell implements Comparable<Cell> {
    // 0.004491556° represents 500m
    // TODO (Jernej Jerin): add setting for 250m
    private static final Tuple2<Double, Double> TOP_LEFT = Tuple.of(41.474937 + 0.004491556 / 2,
            -74.913585 - 0.005986 / 2);
    private static final Tuple2<Double, Double> BOTTOM_RIGHT = Tuple.of(TOP_LEFT.getT1() - 300 * 0.004491556,
            TOP_LEFT.getT2() + 300 * 0.005986);

    private int east;
    private int south;
    public double profitability;
    private MedianOfStream<Integer> medianProfit;
    private Deque<Tuple2<Integer, Long>> tripProfitTime;
    private long lastUpdated;
    private Set<Taxi> taxis;

    public Cell(double latitude, double longitude) {
        this.east = (int)((TOP_LEFT.getT1() - latitude) / 0.004491556);
        this.south = (int)((-TOP_LEFT.getT2() + longitude) / 0.005986);
        this.taxis = new LinkedHashSet<>(100);
        this.tripProfitTime = new ArrayDeque<>();
        this.medianProfit = new MedianOfStream<>();
    }

    public Cell(int east, int south) {
        this.east = east;
        this.south = south;
        this.taxis = new LinkedHashSet<>(100);
        this.tripProfitTime = new ArrayDeque<>();
        this.medianProfit = new MedianOfStream<>();
    }

    public int getEast() {
        return east;
    }

    public void setEast(int east) {
        this.east = east;
    }

    public int getSouth() {
        return south;
    }

    public void setSouth(int south) {
        this.south = south;
    }

    public double getProfitability() {
        return profitability;
    }

    public void setProfitability(double profitability) {
        this.profitability = profitability;
    }

    public MedianOfStream getMedianProfit() {
        return medianProfit;
    }

    public void setMedianProfit(MedianOfStream medianProfit) {
        this.medianProfit = medianProfit;
    }

    public Deque<Tuple2<Integer, Long>> getTripProfitTime() {
        return tripProfitTime;
    }

    public void setTripProfitTime(Deque<Tuple2<Integer, Long>> tripProfitTime) {
        this.tripProfitTime = tripProfitTime;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public Set<Taxi> getTaxis() {
        return taxis;
    }

    public void setTaxis(Set<Taxi> taxis) {
        this.taxis = taxis;
    }

    /**
     * Checks if coordinates are inside defined grid. It checks by comparing
     * the coordinates to the top left vs the top right point.
     *
     * @param latitude latitude of the coordinate
     * @param longitude longitude of the coordinate
     * @return a boolean value if the coordinate lies inside grid
     */
    public static boolean inGrid(double latitude, double longitude) {
        return latitude <= TOP_LEFT.getT1() && longitude >= TOP_LEFT.getT2() &&
                latitude >= BOTTOM_RIGHT.getT1() && longitude <= BOTTOM_RIGHT.getT2();
    }

    @Override
    /**
     * Compute hash code by using Apache Commons Lang HashCodeBuilder.
     */
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(this.south)
                .append(this.east)
                .toHashCode();
    }

    @Override
    /**
     * Compute equals by using Apache Commons Lang EqualsBuilder.
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof Cell))
            return false;
        if (obj == this)
            return true;

        Cell cell = (Cell) obj;
        return new EqualsBuilder()
                .append(this.south, cell.south)
                .append(this.east, cell.east)
                .isEquals();
    }

    public double toLatitude() {
        return  TOP_LEFT.getT1() - this.east * 0.004491556;
    }

    public double toLongitude() {
        return this.south * 0.005986 + TOP_LEFT.getT2();
    }

    @Override
    public int compareTo(Cell cell) {
        /* Question 7: How should we order elements in a list that have the same value for the ordering criterion?
            Answer: You should always put the freshest information first. E.g. if route A and B have the same
            frequency, put the route with the freshest input information fist (i.e. the one which includes
            the freshest event).*/
        if (this.profitability < cell.profitability)
            return -1;
        else if (this.profitability > cell.profitability)
            return 1;
        else {
            // if contains drop off timestamps, order by last timestamp in drop off
            // the highest timestamp has preceding
            if (this.lastUpdated < cell.lastUpdated)
                return -1;
            else if (this.lastUpdated > cell.lastUpdated)
                return 1;
            else
                return 0;
        }
    }
}
