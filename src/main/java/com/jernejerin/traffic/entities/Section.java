package com.jernejerin.traffic.entities;

/**
 * Created by Jernej Jerin on 10.4.2015.
 */
public class Section {
    private int id;
    private Road road;
    private Intersection source;
    private Intersection destination;
    private double length;

    public Section(int id, Road road, Intersection source, Intersection destination, double length) {
        this.id = id;
        this.road = road;
        this.source = source;
        this.destination = destination;
        this.length = length;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Road getRoad() {
        return road;
    }

    public void setRoad(Road road) {
        this.road = road;
    }

    public Intersection getSource() {
        return source;
    }

    public void setSource(Intersection source) {
        this.source = source;
    }

    public Intersection getDestination() {
        return destination;
    }

    public void setDestination(Intersection destination) {
        this.destination = destination;
    }

    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }
}
