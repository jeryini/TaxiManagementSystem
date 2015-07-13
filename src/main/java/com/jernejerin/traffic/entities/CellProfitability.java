package com.jernejerin.traffic.entities;

import com.jernejerin.traffic.helper.MedianOfStream;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A value class that holds profitability of the cells.
 *
 * <b>
 *     Note: this class has a natural ordering that is inconsistent with equals..
 *
 * @author Jernej Jerin
 */
public class CellProfitability implements Comparable<CellProfitability> {
    private final Cell cell;
    private int id;
    private int emptyTaxis;
    private double medianProfit;
    private double profitability;
    private MedianOfStream<Float> medianProfitCell;

    public CellProfitability(Cell cell, int id, int emptyTaxis, double medianProfit, double profitability) {
        this.cell = cell;
        this.id = id;
        this.emptyTaxis = emptyTaxis;
        this.medianProfit = medianProfit;
        this.profitability = profitability;
    }

    public CellProfitability(Cell cell, int id, int emptyTaxis, float profit) {
        this.cell = cell;
        this.id = id;
        this.emptyTaxis = emptyTaxis;
        this.medianProfitCell = new MedianOfStream<Float>(profit);
    }

    public Cell getCell() {
        return cell;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getEmptyTaxis() {
        return emptyTaxis;
    }

    public void setEmptyTaxis(int emptyTaxis) {
        this.emptyTaxis = emptyTaxis;
    }

    public double getMedianProfit() {
        return medianProfit;
    }

    public void setMedianProfit(double medianProfit) {
        this.medianProfit = medianProfit;
    }

    public void setProfitability(double profitability) {
        this.profitability = profitability;
    }

    public double getProfitability() {
        return profitability;
    }

    public MedianOfStream<Float> getMedianProfitCell() {
        return medianProfitCell;
    }

    public void setMedianProfitCell(MedianOfStream<Float> medianProfitCell) {
        this.medianProfitCell = medianProfitCell;
    }

    @Override
    /**
     * Compute hash code by using Apache Commons Lang HashCodeBuilder.
     */
    public int hashCode() {
        return new HashCodeBuilder(73, 79)
                .append(this.cell)
                .append(this.id)
                .append(this.emptyTaxis)
                .append(this.medianProfit)
                .append(this.profitability)
                .toHashCode();
    }

    @Override
    /**
     * Compute equals by using Apache Commons Lang EqualsBuilder.
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof CellProfitability))
            return false;
        if (obj == this)
            return true;

        CellProfitability cellProfitability = (CellProfitability) obj;
        return new EqualsBuilder()
                .append(this.cell, cellProfitability.cell)
                .append(this.id, cellProfitability.id)
                .append(this.emptyTaxis, cellProfitability.emptyTaxis)
                .append(this.medianProfit, cellProfitability.medianProfit)
                .append(this.profitability, cellProfitability.profitability)
                .isEquals();
    }

    @Override
    public int compareTo(CellProfitability cellProfitability) {
        /* Question 7: How should we order elements in a list that have the same value for the ordering criterion?
            Answer: You should always put the freshest information first. E.g. if route A and B have the same
            frequency, put the route with the freshest input information fist (i.e. the one which includes
            the freshest event).*/
        if (this.profitability < cellProfitability.profitability)
            return -1;
        else if (this.profitability > cellProfitability.profitability)
            return 1;
        else {
            // if contains drop off timestamps, order by last timestamp in drop off
            // the highest timestamp has preceding
            if (this.id < cellProfitability.id)
                return -1;
            else if (this.id > cellProfitability.id)
                return 1;
            else
                return 0;
        }
    }
}
