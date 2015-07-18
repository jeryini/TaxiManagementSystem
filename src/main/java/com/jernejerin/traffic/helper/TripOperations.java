package com.jernejerin.traffic.helper;

import com.jernejerin.traffic.entities.*;

import org.apache.commons.lang3.math.NumberUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * Helper class for trip operations.
 *
 * @author Jernej Jerin
 */
public class TripOperations {
    private final static Logger LOGGER = Logger.getLogger(TripOperations.class.getName());
    private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Parses and validates a trip for erroneous values. It first checks, if parsed string contains
     * 17 values. If it does not, it returns null.
     *
     * If the value is considered erroneous, it is set to the following values:
     *  - primitive types: MIN_VALUE
     *  - objects: null
     *
     * @param tripValues comma delimited string representing Trip to check
     * @param timestampReceived Timestamp when the event was received
     * @param id id of the event received
     * @return a Trip with erroneous values set to MIN_VALUE, or null if whole trip was malformed
     */
    public static Trip parseValidateTrip(String tripValues, long timestampReceived, int id) {
//        LOGGER.log(Level.INFO, "Started parsing and validating trip = " +
//                tripValues + " from thread = " + Thread.currentThread());

        // our returned trip
        Trip trip = new Trip();

        // values are comma separated
        String[] tripSplit = tripValues.split(",");

        // if we do not have 17 values, then return null
        if (tripSplit.length != 17)
            return null;

        // check for correct values and then set them
        trip.setId(id);
        trip.setMedallion(tryParseMD5(tripSplit[0], null));
        trip.setHackLicense(tryParseMD5(tripSplit[1], null));
        trip.setPickupDatetime(tryParseDateTime(tripSplit[2], null));
        trip.setDropOffDatetime(tryParseDateTime(tripSplit[3], null));
        trip.setDropOffTimestamp(trip.getDropOffDatetime() != null ? trip.getDropOffDatetime().toEpochSecond(ZoneOffset.UTC) * 1000 : 0);
        trip.setTripTime(NumberUtils.toInt(tripSplit[4], Integer.MIN_VALUE));
        trip.setTripDistance(NumberUtils.toFloat(tripSplit[5], Integer.MIN_VALUE));
        trip.setPickupLongitude(tryLongitude(tripSplit[6], Float.MIN_VALUE));
        trip.setPickupLatitude(tryLatitude(tripSplit[7], Float.MIN_VALUE));
        trip.setDropOffLongitude(tryLongitude(tripSplit[8], Float.MIN_VALUE));
        trip.setDropOffLatitude(tryLatitude(tripSplit[9], Float.MIN_VALUE));
        trip.setPaymentType(tryPayment(tripSplit[10], null));
        trip.setFareAmount(NumberUtils.toFloat(tripSplit[11], Float.MIN_VALUE));
        trip.setSurcharge(NumberUtils.toFloat(tripSplit[12], Float.MIN_VALUE));
        trip.setMtaTax(NumberUtils.toFloat(tripSplit[13], Float.MIN_VALUE));
        trip.setTipAmount(NumberUtils.toFloat(tripSplit[14], Float.MIN_VALUE));
        trip.setTollsAmount(NumberUtils.toFloat(tripSplit[15], Float.MIN_VALUE));
        trip.setTotalAmount(NumberUtils.toFloat(tripSplit[16], Float.MIN_VALUE));
        trip.setTimestampReceived(timestampReceived);

        // does the coordinate for pickup location lie inside grid
        if (Cell.inGrid(trip.getPickupLatitude(), trip.getPickupLongitude()) &&
                Cell.inGrid(trip.getDropOffLatitude(), trip.getDropOffLongitude())) {
            trip.setRoute250(new Route(new Cell250(trip.getPickupLatitude(), trip.getPickupLongitude()),
                    new Cell250(trip.getDropOffLatitude(), trip.getDropOffLongitude())));
            trip.setRoute500(new Route(new Cell500(trip.getPickupLatitude(), trip.getPickupLongitude()),
                    new Cell500(trip.getDropOffLatitude(), trip.getDropOffLongitude())));
        }

//        LOGGER.log(Level.INFO, "Finished parsing and validating trip = " +
//                trip.toString() + " from thread = " + Thread.currentThread());
        return trip;
    }

    /**
     * Insert a trip into database.
     *
     * @param trip trip to insert.
     */
    public static void insertTrip(Trip trip) {
        LOGGER.log(Level.INFO, "Started inserting trip into DB for trip = " +
                trip.toString() + " from thread = " + Thread.currentThread());
        PreparedStatement insertTrip = null;
        Connection conn = null;
        try {
            // first we need to get connection from connection pool
            conn = DriverManager.getConnection("jdbc:apache:commons:dbcp:taxi");

            // setting up prepared statement
            insertTrip = conn.prepareStatement("insert into trip (eventId, medallion, hack_license, pickup_datetime, " +
                    "dropoff_datetime, trip_time, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, " +
                    "dropoff_latitude, payment_type, fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, " +
                    "total_amount, timestampReceived) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");


            insertTrip.setInt(1, trip.getId());
            insertTrip.setString(2, trip.getMedallion());
            insertTrip.setString(3, trip.getHackLicense());
            insertTrip.setTimestamp(4, new Timestamp(trip.getPickupDatetime().toEpochSecond(ZoneOffset.UTC) * 1000));
            insertTrip.setTimestamp(5, new Timestamp(trip.getDropOffDatetime().toEpochSecond(ZoneOffset.UTC) * 1000));
            insertTrip.setInt(6, trip.getTripTime());
            insertTrip.setDouble(7, trip.getTripDistance());
            insertTrip.setDouble(8, trip.getPickupLongitude());
            insertTrip.setDouble(9, trip.getPickupLatitude());
            insertTrip.setDouble(10, trip.getDropOffLongitude());
            insertTrip.setDouble(11, trip.getDropOffLatitude());
            insertTrip.setString(12, trip.getPaymentType() != null ? trip.getPaymentType().name() : null);
            insertTrip.setDouble(13, trip.getFareAmount());
            insertTrip.setDouble(14, trip.getSurcharge());
            insertTrip.setDouble(15, trip.getMtaTax());
            insertTrip.setDouble(16, trip.getTipAmount());
            insertTrip.setDouble(17, trip.getTollsAmount());
            insertTrip.setDouble(18, trip.getTotalAmount());
            insertTrip.setLong(19, trip.getTimestampReceived());

            insertTrip.execute();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Problem when inserting ticket into DB for ticket = " +
                    trip + " from thread = " + Thread.currentThread());
        } finally {
            try {
                if (insertTrip != null) insertTrip.close();
            }
            catch(Exception e) {
                LOGGER.log(Level.SEVERE, "Problem with closing prepared statement for ticket = " +
                        trip + " from thread = " + Thread.currentThread());
            }
            try {
                if (conn != null) conn.close();
            }
            catch(Exception e) {
                LOGGER.log(Level.SEVERE, "Problem with closing connection from thread = " + Thread.currentThread());
            }
            LOGGER.log(Level.INFO, "Finished inserting ticket into DB for for ticket = " +
                    trip + " from thread = " + Thread.currentThread());
        }
    }

    /**
     * Checks if the passed string is a valid MD5 checksum string. If it is valid it returns
     * passed in string, otherwise null.
     *
     * @param md5 the MD5 checksum string
     * @param defaultValue the default value to return if parse fails
     * @return MD5 checksum or null if invalid
     */
    public static String tryParseMD5(String md5, String defaultValue) {
        if (md5 != null && md5.matches("[a-fA-F0-9]{32}"))
            return md5;
        else
            return defaultValue;
    }

    /**
     * Parses passed date time of the pattern "yyyy-MM-dd HH:mm:ss". If the
     * string representation is malformed then it returns null.
     *
     * @param dateTime the string representation of date time
     * @param defaultValue the default value to return if parse fails
     * @return parsed date time or null if string representation was null
     */
    public static LocalDateTime tryParseDateTime(String dateTime, LocalDateTime defaultValue) {
        if (dateTime == null)
            return defaultValue;
        try {
            return LocalDateTime.parse(dateTime, formatter);
        } catch (DateTimeParseException e) {
            return defaultValue;
        }
    }

    /**
     * Check if longitude is between -180° and 180° inclusive.
     *
     * @param longitudeStr string representation of longitude
     * @param defaultValue the default value to return if parse fails
     * @return converted longitude
     */
    public static float tryLongitude(String longitudeStr, float defaultValue) {
        if (longitudeStr == null)
            return defaultValue;
        float longitude = NumberUtils.toFloat(longitudeStr, defaultValue);
        if (longitude > 180 || longitude < -180)
            return defaultValue;
        else
            return longitude;
    }

    /**
     * Check if latitude is between -90° and 90° inclusive.
     *
     * @param latitudeStr string representation of latitude
     * @param defaultValue the default value to return if parse fails
     * @return converted latitude
     */
    public static float tryLatitude(String latitudeStr, float defaultValue) {
        if (latitudeStr == null)
            return defaultValue;
        float latitude = NumberUtils.toFloat(latitudeStr, defaultValue);
        if (latitude > 90 || latitude < -90)
            return defaultValue;
        else
            return latitude;
    }

    /**
     * Check if payment is either CASH or CREDIT CARD.
     *
     * @param payment string representation of payment
     * @param defaultValue the default value to return if parse fails
     * @return converted payment
     */
    public static Payment tryPayment(String payment, Payment defaultValue) {
        if (payment == null)
            return defaultValue;
        if (payment.equals("CSH"))
            return Payment.CASH;
        if (payment.equals("CRD"))
            return Payment.CREDIT_CARD;
        else
            return defaultValue;
    }

}
