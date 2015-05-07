package com.jernejerin.traffic.entities;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

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
public class Cell {
    // 0.004491556° represents 500m
    private static final Tuple2<Double, Double> TOP_LEFT = Tuple.of(41.474937 + 0.004491556 / 2,
            -74.913585 - 0.005986 / 2);
    private static final Tuple2<Double, Double> BOTTOM_RIGHT = Tuple.of(TOP_LEFT.getT1() - 300 * 0.004491556,
            TOP_LEFT.getT2() + 300 * 0.005986);

    private int east;
    private int south;

    public Cell(double latitude, double longitude) {
        this.east = (int)((TOP_LEFT.getT1() - latitude) / 0.004491556);
        this.south = (int)((-TOP_LEFT.getT2() + longitude) / 0.005986);
    }

    public Cell(int east, int south) {
        this.east = east;
        this.south = south;
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
        // TODO (Jernej Jerin): Evaluate using the solution without using HashCodeBuilder
//        int hash = 17;
//        hash = ((hash + this.east) << 5) - (hash + this.east);
//        hash = ((hash + this.south) << 5) - (hash + this.south);
//        return hash;
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
}
