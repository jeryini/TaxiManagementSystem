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

    public static CellProfit fromTrip(Trip trip) {
        return new CellProfit(trip.getId(), new MedianOfStream<>(trip.getFareAmount() +
                trip.getTipAmount()));
    }

    public static CellProfit combine(CellProfit cellProfit1, CellProfit cellProfit2) {
        CellProfit recent = cellProfit1.id > cellProfit2.id ? cellProfit1 : cellProfit2;

        // combine the median profits
        recent.medianProfit.maxHeap.forEach(cellProfit2.medianProfit::addNumberToStream);
        recent.medianProfit.minHeap.forEach(cellProfit2.medianProfit::addNumberToStream);

        return new CellProfit(recent.id, recent.medianProfit);
    }
}
