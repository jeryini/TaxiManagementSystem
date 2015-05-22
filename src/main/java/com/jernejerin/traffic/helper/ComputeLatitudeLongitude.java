package com.jernejerin.traffic.helper;

import com.jernejerin.traffic.entities.Cell;
import com.jernejerin.traffic.entities.Cell250;
import com.jernejerin.traffic.entities.Cell500;

/**
 * Created by Jernej on 7.5.2015.
 */
public class ComputeLatitudeLongitude {
    public static void main(String[] args) {
        // a very common cell in top 10 routes
        // http://www.latlong.net/c/?lat=40.749554&long=-73.988739
        // Center of Manhattan looks like a very probable location for a high taxi route
        Cell cell = new Cell500(155, 162);
        Cell cellEnd = new Cell250(40.758236f, -73.962875f);
        Cell cellProfit = new Cell250(296, 326);


        System.out.printf("Latitude: %f, longitude: %f%n", cell.toLatitude(), cell.toLongitude());
        System.out.printf("Latitude: %d, longitude: %d%n", cellEnd.getEast(), cellEnd.getSouth());
        System.out.printf("Latitude: %f, longitude: %f%n", cellProfit.toLatitude(), cellProfit.toLongitude());

    }
}