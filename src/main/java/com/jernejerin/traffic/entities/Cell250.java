package com.jernejerin.traffic.entities;

/**
 * Represents a square of 250m X 250m in a grid. See class Cell for more details.
 *
 * @author Jernej Jerin
 */
public class Cell250 extends Cell {
    public Cell250(int east, int south) {
        this.east = east;
        this.south = south;
    }

    public Cell250(float latitude, float longitude) {
        this.east = (int)((-TOP_LEFT.getT2() + longitude) / EAST_250);
        this.south = (int)((TOP_LEFT.getT1() - latitude) / SOUTH_250);
    }

    public float toLatitude() {
        return TOP_LEFT.getT1() - this.south * SOUTH_250;
    }

    public float toLongitude() {
        return this.east * EAST_250 + TOP_LEFT.getT2();
    }
}
