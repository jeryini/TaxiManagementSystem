package com.jernejerin.traffic.entities;

/**
 * Created by Jernej Jerin on 11.4.2015.
 */
public class BaseTicket {
    private int id;
    private long startTime;
    private long lastUpdated;
    private double speed;
    private int currentLaneId;
    private int previousSectionId;
    private int nextSectionId;
    private double sectionPositon;
    private int destinationId;
    private int vehicleId;

    public BaseTicket() {}

    public BaseTicket(int id, long startTime, long lastUpdated, double speed, int currentLaneId, int previousSectionId, int nextSectionId, double sectionPositon, int destinationId, int vehicleId) {
        this.id = id;
        this.startTime = startTime;
        this.lastUpdated = lastUpdated;
        this.speed = speed;
        this.currentLaneId = currentLaneId;
        this.previousSectionId = previousSectionId;
        this.nextSectionId = nextSectionId;
        this.sectionPositon = sectionPositon;
        this.destinationId = destinationId;
        this.vehicleId = vehicleId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int getCurrentLaneId() {
        return currentLaneId;
    }

    public void setCurrentLaneId(int currentLaneId) {
        this.currentLaneId = currentLaneId;
    }

    public int getPreviousSectionId() {
        return previousSectionId;
    }

    public void setPreviousSectionId(int previousSectionId) {
        this.previousSectionId = previousSectionId;
    }

    public int getNextSectionId() {
        return nextSectionId;
    }

    public void setNextSectionId(int nextSectionId) {
        this.nextSectionId = nextSectionId;
    }

    public double getSectionPositon() {
        return sectionPositon;
    }

    public void setSectionPositon(double sectionPositon) {
        this.sectionPositon = sectionPositon;
    }

    public int getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(int destinationId) {
        this.destinationId = destinationId;
    }

    public int getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public void setBaseTicket(int id, long startTime, long lastUpdated, double speed, int currentLaneId,
                              int previousSectionId, int nextSectionId, double sectionPositon, int destinationId,
                              int vehicleId) {
        this.id = id;
        this.startTime = startTime;
        this.lastUpdated = lastUpdated;
        this.speed = speed;
        this.currentLaneId = currentLaneId;
        this.previousSectionId = previousSectionId;
        this.nextSectionId = nextSectionId;
        this.sectionPositon = sectionPositon;
        this.destinationId = destinationId;
        this.vehicleId = vehicleId;
    }
}
