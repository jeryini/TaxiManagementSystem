package com.jernejerin.traffic.entities;

/**
 * Created by Jernej Jerin on 10.4.2015.
 */
public class Ticket extends BaseTicket {
    private Lane lane;
    private Section previousSection;
    private Section nextSection;
    private Intersection destination;
    private Vehicle vehicle;

    public Ticket(BaseTicket baseTicket, Lane lane, Section previousSection,
                  Section nextSection, Intersection destination, Vehicle vehicle) {
        super(baseTicket.getId(), baseTicket.getStartTime(), baseTicket.getLastUpdated(),
                baseTicket.getSpeed(), lane.getId(), previousSection.getId(),
                nextSection.getId(), baseTicket.getSectionPositon(), destination.getId(),
                vehicle.getId());
        this.lane = lane;
        this.previousSection = previousSection;
        this.nextSection = nextSection;
        this.destination = destination;
        this.vehicle = vehicle;
    }

    public Lane getLane() {
        return lane;
    }

    public void setLane(Lane lane) {
        this.lane = lane;
    }

    public Section getPreviousSection() {
        return previousSection;
    }

    public void setPreviousSection(Section previousSection) {
        this.previousSection = previousSection;
    }

    public Section getNextSection() {
        return nextSection;
    }

    public void setNextSection(Section nextSection) {
        this.nextSection = nextSection;
    }

    public Intersection getDestination() {
        return destination;
    }

    public void setDestination(Intersection destination) {
        this.destination = destination;
    }

    public Vehicle getVehicle() {
        return vehicle;
    }

    public void setVehicle(Vehicle vehicle) {
        this.vehicle = vehicle;
    }
}
