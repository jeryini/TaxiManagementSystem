package com.jernejerin.traffic.database;

import com.fasterxml.jackson.databind.deser.Deserializers;
import com.jernejerin.traffic.entities.BaseTicket;
import com.jernejerin.traffic.entities.Ticket;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
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
     * @param baseTicket Ticket to check.
     * @return A base ticket with erroneous values set to -1.
     */
    public static BaseTicket validateTicketValues(BaseTicket baseTicket) {
        LOGGER.log(Level.INFO, "Started validating ticket values for ticket id = " +
                baseTicket.getId() + " from thread = " + Thread.currentThread());
        if (baseTicket.getId() < 0) {
            baseTicket.setId(-1);
        }
        if (baseTicket.getStartTime() < 0) {
            baseTicket.setStartTime(-1);
        }
        if (baseTicket.getLastUpdated() < 0) {
            baseTicket.setLastUpdated(-1);
        }
        if (baseTicket.getSpeed() < 0 || baseTicket.getSpeed() > 100) {
            baseTicket.setSpeed(-1);
        }
        if (baseTicket.getCurrentLaneId() < 0) {
            baseTicket.setCurrentLaneId(-1);
        }
        if (baseTicket.getPreviousSectionId() < 0) {
            baseTicket.setPreviousSectionId(-1);
        }
        if (baseTicket.getNextSectionId() < 0) {
            baseTicket.setNextSectionId(-1);
        }
        if (baseTicket.getSectionPositon() < 0) {
            baseTicket.setSectionPositon(-1);
        }
        if (baseTicket.getDestinationId() < 0) {
            baseTicket.setDestinationId(-1);
        }
        if (baseTicket.getVehicleId() < 0) {
            baseTicket.setVehicleId(-1);
        }
        LOGGER.log(Level.INFO, "Finished validating ticket values for ticket id = " +
                baseTicket.getId() + " from thread = " + Thread.currentThread());
        return baseTicket;
    }

    /**
     * Insert a ticket into database.
     *
     * @param baseTicket Ticket to insert.
     */
    public static void insertTicket(BaseTicket baseTicket) {
        LOGGER.log(Level.INFO, "Started inserting ticket into DB for ticket id = " +
                baseTicket.getId() + " from thread = " + Thread.currentThread());
        PreparedStatement insertTicket = null;
        Connection conn = null;
        try {
            // first we need to get connection from connection pool
            conn = DriverManager.getConnection("jdbc:apache:commons:dbcp:example");

            // setting up prepared statement
            insertTicket = conn.prepareStatement("insert into ticket (id, " +
                    "startTime, lastUpdated, speed, currentLaneId, previousSectionId, " +
                    "nextSectionId, sectionPosition, destinationId, vehicleId) values (?, ?, ?, " +
                    "?, ?, ?, ?, ?, ?, ?)");

            insertTicket.setInt(1, baseTicket.getId());
            insertTicket.setLong(2, baseTicket.getStartTime());
            insertTicket.setLong(3, baseTicket.getLastUpdated());
            insertTicket.setDouble(4, baseTicket.getSpeed());
            insertTicket.setInt(5, baseTicket.getCurrentLaneId());
            insertTicket.setInt(6, baseTicket.getPreviousSectionId());
            insertTicket.setInt(7, baseTicket.getNextSectionId());
            insertTicket.setDouble(8, baseTicket.getSectionPositon());
            insertTicket.setInt(9, baseTicket.getDestinationId());
            insertTicket.setInt(10, baseTicket.getVehicleId());

            insertTicket.execute();
        } catch (SQLException e) {
            LOGGER.log(Level.INFO, "Problem when inserting ticket into DB for ticket id = " +
                    baseTicket.getId() + " from thread = " + Thread.currentThread());
        } finally {
            try { if (insertTicket != null) insertTicket.close(); } catch(Exception e) { }
            try { if (conn != null) conn.close(); } catch(Exception e) { }
            LOGGER.log(Level.INFO, "Finished inserting ticket into DB for for ticket id = " +
                    baseTicket.getId() + " from thread = " + Thread.currentThread());
        }
    }
}
