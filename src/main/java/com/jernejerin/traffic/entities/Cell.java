package com.jernejerin.traffic.entities;

import com.jernejerin.traffic.helper.MedianOfStream;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

import java.util.*;

/**
 * <p>
 * Represents a square in a grid. The cell
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
public abstract class Cell {
    // 0.004491556° represents 500m change to south
    protected static final float SOUTH_250 = 0.004491556f / 2f;
    protected static final float SOUTH_500 = 0.004491556f;

    // 0.005986° represents 500m change to east
    protected static final float EAST_250 = 0.005986f / 2f;
    protected static final float EAST_500 = 0.005986f;

    // center of the first cell (0.0)
    private static final Tuple2<Float, Float> CENTER_CELL_1_1 = Tuple.of(41.474937f, -74.913585f);

    protected static final Tuple2<Float, Float> TOP_LEFT =
            Tuple.of(CENTER_CELL_1_1.getT1() + SOUTH_250, CENTER_CELL_1_1.getT2() - EAST_250);
    protected static final Tuple2<Float, Float> BOTTOM_RIGHT =
            Tuple.of(TOP_LEFT.getT1() - 300f * SOUTH_500, TOP_LEFT.getT2() + 300f * EAST_500);

    protected int east;
    protected int south;

    public Cell() {}

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

    /**
     * Checks if coordinates are inside defined grid. It checks by comparing
     * the coordinates to the top left vs the top right point.
     *
     * @param latitude latitude of the coordinate
     * @param longitude longitude of the coordinate
     * @return a boolean value if the coordinate lies inside grid
     */
    public static boolean inGrid(float latitude, float longitude) {
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

    public abstract float toLatitude();

    public abstract float toLongitude();

    @Override
    public String toString() {
        return this.east + "." + this.south;
    }
}
