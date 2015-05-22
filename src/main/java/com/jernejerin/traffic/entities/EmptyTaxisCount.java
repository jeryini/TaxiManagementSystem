package com.jernejerin.traffic.entities;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created by Jernej on 22.5.2015.
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

    public static EmptyTaxisCount fromTrip(Trip trip) {
        return new EmptyTaxisCount(trip.getId(), 1);
    }

    public static EmptyTaxisCount combine(EmptyTaxisCount emptyTaxisCount1, EmptyTaxisCount emptyTaxisCount2) {
        return new EmptyTaxisCount(emptyTaxisCount1.id > emptyTaxisCount2.id ? emptyTaxisCount1.id :
                emptyTaxisCount2.id, emptyTaxisCount1.count + emptyTaxisCount2.count);
    }
}
