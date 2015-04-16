package com.jernejerin.traffic.entities;

/**
 * Created by Jernej Jerin on 10.4.2015.
 */
public class Lane {
    private int id;
    private Section section;

    public Lane(int id, Section section) {
        this.id = id;
        this.section = section;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Section getSection() {
        return section;
    }

    public void setSection(Section section) {
        this.section = section;
    }
}
