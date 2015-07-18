package com.jernejerin.traffic.entities;

import java.time.LocalDateTime;

/**
 * A taxi trip entity. Stores the trip data, such as fare, location, identity etc.
 *
 * @author Jernej Jerin
 */
public class Trip {

    private int id;        // consecutive id of the Trip
    private String medallion;  // an md5sum identifier of the taxi - vehicle bound
    private String hackLicense; // an md5sum identifier of the taxi license
    private LocalDateTime pickupDatetime; // time and date when the passenger(s) were picked up
    private LocalDateTime dropOffDatetime; // time and date when the passenger(s) were dropped off
    private long pickupTimestamp;   // pickup datetime represented as timestamp
    private long dropOffTimestamp;  // pickup datetime represented as timestamp
    private int tripTime;   // duration of the trip in seconds
    private float tripDistance;    // trip distance in miles
    private float pickupLongitude; // longitude coordinate of the pickup location
    private float pickupLatitude;  // latitude coordinate of the pickup location
    private float dropOffLongitude;    // longitude coordinate of the drop-off location
    private float dropOffLatitude; // latitude coordinate of the drop-off location
    private Payment paymentType;    // the payment method - credit card or cash
    private float fareAmount;    // fare amount in dollars
    private float surcharge;  // surcharge in dollars
    private float mtaTax; // tax in dollars
    private float tipAmount;  // tip in dollars
    private float tollsAmount; // bridge and tunnel tolls in dollars
    private float totalAmount; // total paid amount in dollars
    private long timestampReceived; // timestamp in milliseconds when we received the event
    // event that triggered the output and the time when the output is produced
    private Route route500;    // route between start cell and end cell with 500m X 500m
    private Route route250;    // route between start cell and end cell with 250m X 250m

    public Trip() {}

    public Trip(int id, String medallion, String hackLicense, LocalDateTime pickupDatetime,
                LocalDateTime dropOffDatetime, long pickupTimestamp, long dropOffTimestamp,
                int tripTime, float tripDistance, float pickupLongitude, float pickupLatitude,
                float dropOffLongitude, float dropOffLatitude, Payment paymentType, float fareAmount,
                float surcharge, float mtaTax, float tipAmount, float tollsAmount, float totalAmount,
                long timestampReceived, Route route500, Route route250) {
        this.id = id;
        this.medallion = medallion;
        this.hackLicense = hackLicense;
        this.pickupDatetime = pickupDatetime;
        this.dropOffDatetime = dropOffDatetime;
        this.pickupTimestamp = pickupTimestamp;
        this.dropOffTimestamp = dropOffTimestamp;
        this.tripTime = tripTime;
        this.tripDistance = tripDistance;
        this.pickupLongitude = pickupLongitude;
        this.pickupLatitude = pickupLatitude;
        this.dropOffLongitude = dropOffLongitude;
        this.dropOffLatitude = dropOffLatitude;
        this.paymentType = paymentType;
        this.fareAmount = fareAmount;
        this.surcharge = surcharge;
        this.mtaTax = mtaTax;
        this.tipAmount = tipAmount;
        this.tollsAmount = tollsAmount;
        this.totalAmount = totalAmount;
        this.timestampReceived = timestampReceived;
        this.route500 = route500;
        this.route250 = route250;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getMedallion() {
        return medallion;
    }

    public void setMedallion(String medallion) {
        this.medallion = medallion;
    }

    public String getHackLicense() {
        return hackLicense;
    }

    public void setHackLicense(String hackLicense) {
        this.hackLicense = hackLicense;
    }

    public LocalDateTime getPickupDatetime() {
        return pickupDatetime;
    }

    public void setPickupDatetime(LocalDateTime pickupDatetime) {
        this.pickupDatetime = pickupDatetime;
    }

    public LocalDateTime getDropOffDatetime() {
        return dropOffDatetime;
    }

    public void setDropOffDatetime(LocalDateTime dropOffDatetime) {
        this.dropOffDatetime = dropOffDatetime;
    }

    public long getPickupTimestamp() {
        return pickupTimestamp;
    }

    public void setPickupTimestamp(long pickupTimestamp) {
        this.pickupTimestamp = pickupTimestamp;
    }

    public long getDropOffTimestamp() {
        return dropOffTimestamp;
    }

    public void setDropOffTimestamp(long dropOffTimestamp) {
        this.dropOffTimestamp = dropOffTimestamp;
    }

    public int getTripTime() {
        return tripTime;
    }

    public void setTripTime(int tripTime) {
        this.tripTime = tripTime;
    }

    public float getTripDistance() {
        return tripDistance;
    }

    public void setTripDistance(float tripDistance) {
        this.tripDistance = tripDistance;
    }

    public float getPickupLongitude() {
        return pickupLongitude;
    }

    public void setPickupLongitude(float pickupLongitude) {
        this.pickupLongitude = pickupLongitude;
    }

    public float getPickupLatitude() {
        return pickupLatitude;
    }

    public void setPickupLatitude(float pickupLatitude) {
        this.pickupLatitude = pickupLatitude;
    }

    public float getDropOffLongitude() {
        return dropOffLongitude;
    }

    public void setDropOffLongitude(float dropOffLongitude) {
        this.dropOffLongitude = dropOffLongitude;
    }

    public float getDropOffLatitude() {
        return dropOffLatitude;
    }

    public void setDropOffLatitude(float dropOffLatitude) {
        this.dropOffLatitude = dropOffLatitude;
    }

    public Payment getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(Payment paymentType) {
        this.paymentType = paymentType;
    }

    public float getFareAmount() {
        return fareAmount;
    }

    public void setFareAmount(float fareAmount) {
        this.fareAmount = fareAmount;
    }

    public float getSurcharge() {
        return surcharge;
    }

    public void setSurcharge(float surcharge) {
        this.surcharge = surcharge;
    }

    public float getMtaTax() {
        return mtaTax;
    }

    public void setMtaTax(float mtaTax) {
        this.mtaTax = mtaTax;
    }

    public float getTipAmount() {
        return tipAmount;
    }

    public void setTipAmount(float tipAmount) {
        this.tipAmount = tipAmount;
    }

    public float getTollsAmount() {
        return tollsAmount;
    }

    public void setTollsAmount(float tollsAmount) {
        this.tollsAmount = tollsAmount;
    }

    public float getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(float totalAmount) {
        this.totalAmount = totalAmount;
    }

    public long getTimestampReceived() {
        return timestampReceived;
    }

    public void setTimestampReceived(long timestampReceived) {
        this.timestampReceived = timestampReceived;
    }

    public Route getRoute500() {
        return route500;
    }

    public void setRoute500(Route route500) {
        this.route500 = route500;
    }

    public Route getRoute250() {
        return route250;
    }

    public void setRoute250(Route route250) {
        this.route250 = route250;
    }

    public String toString() {
        return this.id + ", " + this.medallion + ", " + this.hackLicense + ", " +
                this.pickupDatetime + ", " + this.dropOffDatetime + ", " + this.pickupTimestamp + ", "
                + this.dropOffTimestamp + ", " + this.tripTime + ", " + this.tripDistance + ", " + this.pickupLongitude
                + ", " + this.pickupLatitude + ", " + this.dropOffLongitude + ", " + this.dropOffLatitude + ", " +
                this.paymentType + ", " + this.fareAmount + ", " + this.surcharge + ", " + this.mtaTax + ", " +
                this.tipAmount + ", " + this.tollsAmount + ", " + this.totalAmount + ", " + this.timestampReceived +
                ", " + this.route500 + ", " + this.route250;
    }
}
