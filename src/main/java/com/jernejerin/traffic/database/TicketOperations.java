package com.jernejerin.traffic.database;

import com.jernejerin.traffic.entities.Trip;

import java.sql.*;
import java.util.logging.Logger;

/**
 * Created by Jernej Jerin on 19.4.2015.
 */
public class TicketOperations {
    private final static Logger LOGGER = Logger.getLogger(TicketOperations.class.getName());

    /**
     * Validates a base ticket for erroneous values. If the value is considered erroneous,
     * it is set to -1.
     *
     * @param trip Ticket to check.
     * @return A base ticket with erroneous values set to -1.
     */
    public static Trip validateTicketValues(Trip trip) {
//        LOGGER.log(Level.INFO, "Started validating ticket values for ticket id = " +
//                trip.getId() + " from thread = " + Thread.currentThread());
//        if (trip.getId() < 0) {
//            trip.setId(-1);
//        }
//        if (trip.getStartTime() < 0) {
//            trip.setStartTime(-1);
//        }
//        if (trip.getLastUpdated() < 0) {
//            trip.setLastUpdated(-1);
//        }
//        if (trip.getSpeed() < 0 || trip.getSpeed() > 100) {
//            trip.setSpeed(-1);
//        }
//        if (trip.getCurrentLaneId() < 0) {
//            trip.setCurrentLaneId(-1);
//        }
//        if (trip.getPreviousSectionId() < 0) {
//            trip.setPreviousSectionId(-1);
//        }
//        if (trip.getNextSectionId() < 0) {
//            trip.setNextSectionId(-1);
//        }
//        if (trip.getSectionPositon() < 0) {
//            trip.setSectionPositon(-1);
//        }
//        if (trip.getDestinationId() < 0) {
//            trip.setDestinationId(-1);
//        }
//        if (trip.getVehicleId() < 0) {
//            trip.setVehicleId(-1);
//        }
//        LOGGER.log(Level.INFO, "Finished validating ticket values for ticket id = " +
//                trip.getId() + " from thread = " + Thread.currentThread());
        return trip;
    }

    /**
     * Insert a ticket into database.
     *
     * @param trip Ticket to insert.
     */
    public static void insertTicket(Trip trip) {
//        LOGGER.log(Level.INFO, "Started inserting ticket into DB for ticket id = " +
//                trip.getId() + " from thread = " + Thread.currentThread());
        PreparedStatement insertTicket = null;
        Connection conn = null;
        try {
            // first we need to get connection from connection pool
            conn = DriverManager.getConnection("jdbc:apache:commons:dbcp:example");

            // setting up prepared statement
            insertTicket = conn.prepareStatement("insert into ticket (startTime, lastUpdated, speed, currentLaneId, " +
                    "previousSectionId, nextSectionId, sectionPosition, destinationId, vehicleId, startProcessing) " +
                    "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

//            insertTicket.setLong(1, trip.getStartTime());
//            insertTicket.setLong(2, trip.getLastUpdated());
//            insertTicket.setDouble(3, trip.getSpeed());
//            insertTicket.setInt(4, trip.getCurrentLaneId());
//            insertTicket.setInt(5, trip.getPreviousSectionId());
//            insertTicket.setInt(6, trip.getNextSectionId());
//            insertTicket.setDouble(7, trip.getSectionPositon());
//            insertTicket.setInt(8, trip.getDestinationId());
//            insertTicket.setInt(9, trip.getVehicleId());
//            insertTicket.setLong(10, trip.getStartProcessing());

            insertTicket.execute();
        } catch (SQLException e) {
//            LOGGER.log(Level.INFO, "Problem when inserting ticket into DB for ticket id = " +
//                    trip.getId() + " from thread = " + Thread.currentThread());
        } finally {
            try { if (insertTicket != null) insertTicket.close(); } catch(Exception e) { }
            try { if (conn != null) conn.close(); } catch(Exception e) { }
//            LOGGER.log(Level.INFO, "Finished inserting ticket into DB for for ticket id = " +
//                    trip.getId() + " from thread = " + Thread.currentThread());
        }
    }

//    public static Vehicle getVehicleById(int vehicleId) {
//        PreparedStatement getVehicle = null;
//        Connection conn = null;
//        try {
//            conn = DriverManager.getConnection("jdbc:apache:commons:dbcp:example");
//
//            // setting prepare statement for getting vehicle
//            getVehicle = conn.prepareStatement("select * from vehicle where id = ?");
//            getVehicle.setInt(1, vehicleId);
//
//
//        }
//    }

    public static void updateTicket(Trip trip) {
//        LOGGER.log(Level.INFO, "Started inserting ticket into DB for ticket id = " +
//                trip.getId() + " from thread = " + Thread.currentThread());
        PreparedStatement updateTicket = null;
        Connection conn = null;
        try {
            // first we need to get connection from connection pool
            conn = DriverManager.getConnection("jdbc:apache:commons:dbcp:example");

            // setting up prepared statement
            updateTicket = conn.prepareStatement("update ticket set finishProcessing = ? where id = ?");

//            updateTicket.setLong(1, trip.getFinishProcessing());
            updateTicket.setInt(2, trip.getId());

            updateTicket.execute();
        } catch (SQLException e) {
//            LOGGER.log(Level.INFO, "Problem when inserting ticket into DB for ticket id = " +
//                    trip.getId() + " from thread = " + Thread.currentThread());
        } finally {
            try { if (updateTicket != null) updateTicket.close(); } catch(Exception e) { }
            try { if (conn != null) conn.close(); } catch(Exception e) { }
//            LOGGER.log(Level.INFO, "Finished inserting ticket into DB for for ticket id = " +
//                    trip.getId() + " from thread = " + Thread.currentThread());
        }
    }
}
