package com.jernejerin.traffic.entities;

import java.time.LocalTime;

/**
 * Created by Jernej Jerin on 11.4.2015.
 */
public class Trip {

    private int id;
    private String medallion;  // an md5sum of the identifier of the taxi - vehicle bound
    private String hackLicense; // an md5sum of the identifier for the taxi license
    private LocalTime pickupDatetime; // time when the passenger(s) were picked up
    private LocalTime dropOffDatetime; // time when the passenger(s) were dropped off
    private int tripTime;   // duration of the trip in seconds
    private double tripDistance;    // trip distance in miles
    private double pickupLongitude; // longitude coordinate of the pickup location
    private double pickupLatitude;  // latitude coordinate of the pickup location
    private double dropOffLongitude;    // longitude coordinate of the drop-off location
    private double dropOffLatitude; // latitude coordinate of the drop-off location
    private Payment paymentType;    // the payment method - credit card or cash
    private int fareAmount;    // fare amount in dollar cents
    private int surcharge;  // surcharge in dollar cents
    private int mtaTax; // tax in dollar cents
    private int tipAmount;  // tip in dollar cents
    private int tollsAmount; // bridge and tunnel tolls in dollar cents
    private int totalAmount; // total paid amount in dollar cents

    public Trip() {}

    public Trip(int id, String medallion, String hackLicense, LocalTime pickupDatetime, LocalTime dropOffDatetime, int tripTime, double tripDistance, double pickupLongitude, double pickupLatitude, double dropOffLongitude, double dropOffLatitude, Payment paymentType, int fareAmount, int surcharge, int mtaTax, int tipAmount, int tollsAmount, int totalAmount) {
        this.id = id;
        this.medallion = medallion;
        this.hackLicense = hackLicense;
        this.pickupDatetime = pickupDatetime;
        this.dropOffDatetime = dropOffDatetime;
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

    public LocalTime getPickupDatetime() {
        return pickupDatetime;
    }

    public void setPickupDatetime(LocalTime pickupDatetime) {
        this.pickupDatetime = pickupDatetime;
    }

    public LocalTime getDropOffDatetime() {
        return dropOffDatetime;
    }

    public void setDropOffDatetime(LocalTime dropOffDatetime) {
        this.dropOffDatetime = dropOffDatetime;
    }

    public int getTripTime() {
        return tripTime;
    }

    public void setTripTime(int tripTime) {
        this.tripTime = tripTime;
    }

    public double getTripDistance() {
        return tripDistance;
    }

    public void setTripDistance(double tripDistance) {
        this.tripDistance = tripDistance;
    }

    public double getPickupLongitude() {
        return pickupLongitude;
    }

    public void setPickupLongitude(double pickupLongitude) {
        this.pickupLongitude = pickupLongitude;
    }

    public double getPickupLatitude() {
        return pickupLatitude;
    }

    public void setPickupLatitude(double pickupLatitude) {
        this.pickupLatitude = pickupLatitude;
    }

    public double getDropOffLongitude() {
        return dropOffLongitude;
    }

    public void setDropOffLongitude(double dropOffLongitude) {
        this.dropOffLongitude = dropOffLongitude;
    }

    public double getDropOffLatitude() {
        return dropOffLatitude;
    }

    public void setDropOffLatitude(double dropOffLatitude) {
        this.dropOffLatitude = dropOffLatitude;
    }

    public Payment getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(Payment paymentType) {
        this.paymentType = paymentType;
    }

    public int getFareAmount() {
        return fareAmount;
    }

    public void setFareAmount(int fareAmount) {
        this.fareAmount = fareAmount;
    }

    public int getSurcharge() {
        return surcharge;
    }

    public void setSurcharge(int surcharge) {
        this.surcharge = surcharge;
    }

    public int getMtaTax() {
        return mtaTax;
    }

    public void setMtaTax(int mtaTax) {
        this.mtaTax = mtaTax;
    }

    public int getTipAmount() {
        return tipAmount;
    }

    public void setTipAmount(int tipAmount) {
        this.tipAmount = tipAmount;
    }

    public int getTollsAmount() {
        return tollsAmount;
    }

    public void setTollsAmount(int tollsAmount) {
        this.tollsAmount = tollsAmount;
    }

    public int getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(int totalAmount) {
        this.totalAmount = totalAmount;
    }

}
