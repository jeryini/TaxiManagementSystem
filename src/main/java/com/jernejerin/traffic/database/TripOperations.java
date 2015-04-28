package com.jernejerin.traffic.database;

import com.jernejerin.traffic.entities.Trip;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Jernej Jerin on 19.4.2015.
 */
public class TripOperations {
    private final static Logger LOGGER = Logger.getLogger(TripOperations.class.getName());
    private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Parses and validates a trip for erroneous values. It first checks, if parsed string contains
     * 17 values. If it does not, it returns null.
     *
     * If the value is considered erroneous, it is set to the following values:
     *  - primitive types: -1
     *  - objects: null
     *
     * @param tripValues comma delimited string representing Trip to check
     * @return a Trip with erroneous values set to -1, or null if whole trip was malformed
     */
    public static Trip parseValidateTrip(String tripValues) {
        LOGGER.log(Level.INFO, "Started parsing trip = " +
                tripValues + " from thread = " + Thread.currentThread());

        // our returned trip
        Trip trip = new Trip();

        // values are comma separated
        String[] tripSplit = tripValues.split(",");

        // if we do not have 17 values, then return null
        if (tripSplit.length != 17)
            return null;

        // check for correct values and then set them
        trip.setMedallion(parseMD5(tripSplit[0]));
        trip.setHackLicense(parseMD5(tripSplit[1]));
        trip.setPickupDatetime(parseDateTime(tripSplit[2]));
        trip.setPickupDatetime(parseDateTime(tripSplit[3]));


        LOGGER.log(Level.INFO, "Finished validating ticket values for ticket id = " +
                trip.getId() + " from thread = " + Thread.currentThread());
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

    /**
     * Checks if the passed string is a valid MD5 checksum string. If it is valid it returns
     * passed in string, otherwise null.
     *
     * @param s the MD5 checksum string
     * @return MD5 checksum or null if invalid
     */
    public static String parseMD5(String s) {
        if (s.matches("[a-fA-F0-9]{32}"))
            return s;
        else
            return null;
    }

    /**
     * Parses passed date time of the pattern "yyyy-MM-dd HH:mm:ss". If the
     * string representation is malformed then it returns null.
     *
     * @param dateTime the string representation of date time
     * @return parsed date time or null if string representation was null
     */
    public static LocalDateTime parseDateTime(String dateTime) {
        try {
            return LocalDateTime.parse(dateTime, formatter);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
