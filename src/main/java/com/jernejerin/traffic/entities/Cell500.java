package com.jernejerin.traffic.entities;

/**
 * Represents a square of 500m X 500m in a grid. See class Cell for more details.
 *
 * @author Jernej Jerin
 */
public class Cell500 extends Cell {
    public Cell500(int east, int south) {
        this.east = east;
        this.south = south;
    }

    public Cell500(float latitude, float longitude) {
        this.east = (int)((-TOP_LEFT.getT2() + longitude) / EAST_500);
        this.south = (int)((TOP_LEFT.getT1() - latitude) / SOUTH_500);
        this.id = this.east * 300 + this.south;
    }

    public float toLatitude() {
        return TOP_LEFT.getT1() - this.south * SOUTH_500;
    }

    public float toLongitude() {
        return this.east * EAST_500 + TOP_LEFT.getT2();
    }
}
