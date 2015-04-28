package com.jernejerin.traffic.entities;

import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Created by Jernej Jerin on 11.4.2015.
 */
public class Trip {

    private int id;
    private String medallion;  // an md5sum of the identifier of the taxi - vehicle bound
    private String hackLicense; // an md5sum of the identifier for the taxi license
    private LocalDateTime pickupDatetime; // time when the passenger(s) were picked up
    private LocalDateTime dropOffDatetime; // time when the passenger(s) were dropped off
    private int tripTime;   // duration of the trip in seconds
    private double tripDistance;    // trip distance in miles
    private double pickupLongitude; // longitude coordinate of the pickup location
    private double pickupLatitude;  // latitude coordinate of the pickup location
    private double dropOffLongitude;    // longitude coordinate of the drop-off location
    private double dropOffLatitude; // latitude coordinate of the drop-off location
    private Payment paymentType;    // the payment method - credit card or cash
    private double fareAmount;    // fare amount in dollars
    private double surcharge;  // surcharge in dollars
    private double mtaTax; // tax in dollars
    private double tipAmount;  // tip in dollars
    private double tollsAmount; // bridge and tunnel tolls in dollars
    private double totalAmount; // total paid amount in dollars

    public Trip() {}

    public Trip(int id, String medallion, String hackLicense, LocalDateTime pickupDatetime, LocalDateTime dropOffDatetime, int tripTime, double tripDistance, double pickupLongitude, double pickupLatitude, double dropOffLongitude, double dropOffLatitude, Payment paymentType, double fareAmount, double surcharge, double mtaTax, double tipAmount, double tollsAmount, double totalAmount) {
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

    public double getFareAmount() {
        return fareAmount;
    }

    public void setFareAmount(double fareAmount) {
        this.fareAmount = fareAmount;
    }

    public double getSurcharge() {
        return surcharge;
    }

    public void setSurcharge(double surcharge) {
        this.surcharge = surcharge;
    }

    public double getMtaTax() {
        return mtaTax;
    }

    public void setMtaTax(double mtaTax) {
        this.mtaTax = mtaTax;
    }

    public double getTipAmount() {
        return tipAmount;
    }

    public void setTipAmount(double tipAmount) {
        this.tipAmount = tipAmount;
    }

    public double getTollsAmount() {
        return tollsAmount;
    }

    public void setTollsAmount(double tollsAmount) {
        this.tollsAmount = tollsAmount;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

}
