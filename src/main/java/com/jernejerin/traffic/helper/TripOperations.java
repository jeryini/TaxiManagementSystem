package com.jernejerin.traffic.helper;

import com.jernejerin.traffic.entities.Payment;
import com.jernejerin.traffic.entities.Trip;

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
 * </p>
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
     * @return a Trip with erroneous values set to MIN_VALUE, or null if whole trip was malformed
     */
    public static Trip parseValidateTrip(String tripValues) {
        LOGGER.log(Level.INFO, "Started parsing and validating trip = " +
                tripValues + " from thread = " + Thread.currentThread());

        // our returned trip
        Trip trip = new Trip();

        // values are comma separated
        String[] tripSplit = tripValues.split(",");

        // if we do not have 17 values, then return null
        if (tripSplit.length != 17)
            return null;

        // check for correct values and then set them
        trip.setMedallion(tryParseMD5(tripSplit[0], null));
        trip.setHackLicense(tryParseMD5(tripSplit[1], null));
        trip.setPickupDatetime(tryParseDateTime(tripSplit[2], null));
        trip.setDropOffDatetime(tryParseDateTime(tripSplit[3], null));
        trip.setTripTime(NumberUtils.toInt(tripSplit[4], Integer.MIN_VALUE));
        trip.setTripDistance(NumberUtils.toDouble(tripSplit[5], Integer.MIN_VALUE));
        trip.setPickupLongitude(tryLongitude(tripSplit[6], Double.MIN_VALUE));
        trip.setPickupLatitude(tryLatitude(tripSplit[7], Double.MIN_VALUE));
        trip.setDropOffLongitude(tryLongitude(tripSplit[8], Double.MIN_VALUE));
        trip.setDropOffLatitude(tryLatitude(tripSplit[9], Double.MIN_VALUE));
        trip.setPaymentType(tryPayment(tripSplit[10], null));
        trip.setFareAmount(NumberUtils.toDouble(tripSplit[11], Double.MIN_VALUE));
        trip.setSurcharge(NumberUtils.toDouble(tripSplit[12], Double.MIN_VALUE));
        trip.setMtaTax(NumberUtils.toDouble(tripSplit[13], Double.MIN_VALUE));
        trip.setTipAmount(NumberUtils.toDouble(tripSplit[14], Double.MIN_VALUE));
        trip.setTollsAmount(NumberUtils.toDouble(tripSplit[15], Double.MIN_VALUE));
        trip.setTotalAmount(NumberUtils.toDouble(tripSplit[16], Double.MIN_VALUE));

        LOGGER.log(Level.INFO, "Finished parsing and validating trip = " +
                trip.toString() + " from thread = " + Thread.currentThread());
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
            conn = DriverManager.getConnection("jdbc:apache:commons:dbcp:example");

            // setting up prepared statement
            insertTrip = conn.prepareStatement("insert into trip (medallion, hack_license, pickup_datetime, " +
                    "dropoff_datetime, trip_time, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, " +
                    "dropoff_latitude, payment_type, fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, " +
                    "total_amount) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            insertTrip.setString(1, trip.getMedallion());
            insertTrip.setString(2, trip.getHackLicense());
            insertTrip.setTimestamp(3, new Timestamp(trip.getPickupDatetime().toEpochSecond(ZoneOffset.UTC) * 1000));
            insertTrip.setTimestamp(4, new Timestamp(trip.getDropOffDatetime().toEpochSecond(ZoneOffset.UTC) * 1000));
            insertTrip.setInt(5, trip.getTripTime());
            insertTrip.setDouble(6, trip.getTripDistance());
            insertTrip.setDouble(7, trip.getPickupLongitude());
            insertTrip.setDouble(8, trip.getPickupLatitude());
            insertTrip.setDouble(9, trip.getDropOffLongitude());
            insertTrip.setDouble(10, trip.getDropOffLatitude());
            insertTrip.setString(11, trip.getPaymentType().name());
            insertTrip.setDouble(12, trip.getFareAmount());
            insertTrip.setDouble(13, trip.getSurcharge());
            insertTrip.setDouble(14, trip.getMtaTax());
            insertTrip.setDouble(15, trip.getTipAmount());
            insertTrip.setDouble(16, trip.getTollsAmount());
            insertTrip.setDouble(17, trip.getTotalAmount());

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
    public static double tryLongitude(String longitudeStr, double defaultValue) {
        if (longitudeStr == null)
            return defaultValue;
        double longitude = NumberUtils.toDouble(longitudeStr, defaultValue);
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
    public static double tryLatitude(String latitudeStr, double defaultValue) {
        if (latitudeStr == null)
            return defaultValue;
        double latitude = NumberUtils.toDouble(latitudeStr, defaultValue);
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
