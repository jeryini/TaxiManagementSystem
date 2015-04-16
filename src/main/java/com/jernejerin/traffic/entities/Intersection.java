package com.jernejerin.traffic.entities;

/**
 * Created by Jernej Jerin on 10.4.2015.
 */
public class Intersection {
    private int id;
    private String location;

    public Intersection(int id, String location) {
        this.id = id;
        this.location = location;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
