package com.jernejerin.traffic.entities;

import com.jernejerin.traffic.helper.MedianOfStream;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A value class that holds profit for Cells and the logic for combining.
 *
 * <b>
 *     Note: this class has a natural ordering that is inconsistent with equals.
 *
 * @author Jernej Jerin
 */
public class CellProfit {
    private final int id;
    private final MedianOfStream<Float> medianProfit;

    public CellProfit(int id, MedianOfStream<Float> medianProfit) {
        this.id = id;
        this.medianProfit = medianProfit;
    }

    public int getId() {
        return id;
    }

    public MedianOfStream<Float> getMedianProfit() {
        return medianProfit;
    }

    /**
     * A new cell profit from the trip.
     *
     * @param trip a trip from which to create a new cell profit
     * @return a new cell profit
     */
    public static CellProfit fromTrip(Trip trip) {
        return new CellProfit(trip.getId(), new MedianOfStream<>(trip.getFareAmount() +
                trip.getTipAmount()));
    }

    /**
     * Combines the cell profits.
     *
     * @param cellProfit1 the first cell profit to combine
     * @param cellProfit2 the second cell profit to combine
     * @return a new combined cell profit
     */
    public static CellProfit combine(CellProfit cellProfit1, CellProfit cellProfit2) {
        CellProfit recent = cellProfit1.id > cellProfit2.id ? cellProfit1 : cellProfit2;

        // combine the median profits into cell profit 1 (it does not matter if we chose cell profit 1 or 2)
        cellProfit2.medianProfit.maxHeap.forEach(cellProfit1.medianProfit::addNumberToStream);
        cellProfit2.medianProfit.minHeap.forEach(cellProfit1.medianProfit::addNumberToStream);

        return new CellProfit(recent.id, cellProfit1.medianProfit);
    }
}
