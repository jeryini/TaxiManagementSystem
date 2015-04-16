package com.jernejerin.traffic.entities;

/**
 * Created by Jernej Jerin on 10.4.2015.
 */
public class Vehicle {
    private int id;
    private Type type;
    private double averageSpeed;
    private double distanceTraveled;

    public Vehicle(int id, Type type, double averageSpeed, double distanceTraveled) {
        this.id = id;
        this.type = type;
        this.averageSpeed = averageSpeed;
        this.distanceTraveled = distanceTraveled;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public double getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public double getDistanceTraveled() {
        return distanceTraveled;
    }

    public void setDistanceTraveled(double distanceTraveled) {
        this.distanceTraveled = distanceTraveled;
    }
}
