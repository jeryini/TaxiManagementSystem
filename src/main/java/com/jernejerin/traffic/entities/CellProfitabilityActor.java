package com.jernejerin.traffic.entities;

import java.io.Serializable;

/**
 * Defines the Cell profitability actor and messages that we can pass between these actors.
 *
 * @author Jernej Jerin
 */
public class CellProfitabilityActor {
    // MESSAGES

    /**
     * Message for getting the top 10 profitable cells from the Actor.
     */
    public static class GetTop10 implements Serializable {
        public static final long serialVersionUID = 1;
    }

    /**
     * Message for returning the top 10 profitable cells from the Actor.
     */
    public static class Top10 implements Serializable {
        public static final long serialVersionUID = 1;
        public final CellProfitability[] top10ProfitableCells;

        public Top10(CellProfitability[] top10ProfitableCells) {
            this.top10ProfitableCells = top10ProfitableCells;
        }
    }

    /**
     * General message to update the given Cell. All other messages inherit from this
     * message.
     */
    public static class UpdateCell implements Serializable {
        public static final long serialVersionUID = 1;
        public final Cell cell;
        public final long cellId;
        public final int tripId;

        public UpdateCell(Cell cell, long cellId, int tripId) {
            this.cell = cell;
            this.cellId = cellId;
            this.tripId = tripId;
        }
    }

    /**
     * Message to increment the number of empty taxis for the given Cell.
     */
    public static class IncrementEmptyTaxis extends UpdateCell implements Serializable {
        public static final long serialVersionUID = 1;

        public IncrementEmptyTaxis(Cell cell, long cellId, int tripId) {
            super(cell, cellId, tripId);
        }
    }

    /**
     * Message to decrement the number of empty taxis for the given Cell.
     */
    public static class DecrementEmptyTaxis extends UpdateCell implements Serializable {
        public static final long serialVersionUID = 1;

        public DecrementEmptyTaxis(Cell cell, long cellId, int tripId) {
            super(cell, cellId, tripId);
        }
    }

    /**
     * Message to add the fare + tip amount to the cell.
     */
    public static class AddFareTipAmount extends UpdateCell implements Serializable {
        public static final long serialVersionUID = 1;
        public final float fareTipAmount;

        public AddFareTipAmount(Cell cell, long cellId, int tripId, float fareTipAmount) {
            super(cell, cellId, tripId);
            this.fareTipAmount = fareTipAmount;
        }
    }

    /**
     * Message to remove the fare + tip amount from the cell.
     */
    public static class RemoveFareTipAmount extends UpdateCell implements Serializable {
        public static final long serialVersionUID = 1;
        public final float fareTipAmount;

        public RemoveFareTipAmount(Cell cell, long cellId, int tripId, float fareTipAmount) {
            super(cell, cellId, tripId);
            this.fareTipAmount = fareTipAmount;
        }
    }

    // DEFINE ACTORS
}
