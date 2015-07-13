package com.jernejerin.traffic.entities;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A value class that holds the empty taxis count and the logic for combining.
 *
 * @author Jernej Jerin
 */
public class EmptyTaxisCount {
    private final int id;
    private final int count;

    public EmptyTaxisCount(int id, int count) {
        this.id = id;
        this.count = count;
    }

    public int getId() {
        return id;
    }

    public int getCount() {
        return count;
    }

    /**
     * Creates the empty taxis count from he trip.
     *
     * @param trip the trip from which we create the empty taxis count
     * @return new empty taxis count with count field set to 1
     */
    public static EmptyTaxisCount fromTrip(Trip trip) {
        return new EmptyTaxisCount(trip.getId(), 1);
    }

    /**
     * Combine the empty taxis count.
     *
     * @param emptyTaxisCount1 the first one
     * @param emptyTaxisCount2 the second one
     * @return a new empty taxis count with the largest of the id and a sum of the count
     */
    public static EmptyTaxisCount combine(EmptyTaxisCount emptyTaxisCount1, EmptyTaxisCount emptyTaxisCount2) {
        return new EmptyTaxisCount(emptyTaxisCount1.id > emptyTaxisCount2.id ? emptyTaxisCount1.id :
                emptyTaxisCount2.id, emptyTaxisCount1.count + emptyTaxisCount2.count);
    }
}
