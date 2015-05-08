package com.jernejerin.traffic.helper;

import com.jernejerin.traffic.entities.Cell;

/**
 * Created by Jernej on 7.5.2015.
 */
public class ComputeLatitudeLongitude {
    public static void main(String[] args) {
        Cell cellStart = new Cell(161, 154);
        Cell cellEnd = new Cell(161, 156);

        System.out.printf("Latitude start: %f, longitude start: %f", cellStart.toLatitude(), cellStart.toLongitude());
        System.out.printf("Latitude end: %f, longitude end: %f", cellEnd.toLatitude(), cellEnd.toLongitude());

    }
}
