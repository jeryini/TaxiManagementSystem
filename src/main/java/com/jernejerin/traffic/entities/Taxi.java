package com.jernejerin.traffic.entities;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created by Jernej on 8.5.2015.
 */
public class Taxi {
    private long dropOffTimestamp;
    private String medallion;

    public Taxi(long dropOffTimestamp, String medallion) {
        this.dropOffTimestamp = dropOffTimestamp;
        this.medallion = medallion;
    }

    public long getDropOffTimestamp() {
        return dropOffTimestamp;
    }

    public void setDropOffTimestamp(long dropOffTimestamp) {
        this.dropOffTimestamp = dropOffTimestamp;
    }

    public String getMedallion() {
        return medallion;
    }

    public void setMedallion(String medallion) {
        this.medallion = medallion;
    }

    @Override
    /**
     * Compute hash code by using Apache Commons Lang HashCodeBuilder.
     */
    public int hashCode() {
        return new HashCodeBuilder(43, 59)
                .append(this.medallion)
                .toHashCode();
    }

    @Override
    /**
     * Compute equals by using Apache Commons Lang EqualsBuilder.
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof Taxi))
            return false;
        if (obj == this)
            return true;

        Taxi taxi = (Taxi) obj;
        return new EqualsBuilder()
                .append(this.medallion, taxi.medallion)
                .isEquals();
    }
}
