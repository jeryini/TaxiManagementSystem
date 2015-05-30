package com.jernejerin.traffic.entities;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.aliasi.util.BoundedPriorityQueue;

import java.io.Serializable;
import java.util.Comparator;

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
    public static class GetTop10EmptyTaxis implements Serializable {
        public static final long serialVersionUID = 1;
    }

    public static class GetTop10FareTipAmount implements Serializable {
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

    public static class Top10EmptyTaxis extends Top10 implements Serializable {
        public static final long serialVersionUID = 1;

        public Top10EmptyTaxis(CellProfitability[] top10ProfitableCells) {
            super(top10ProfitableCells);
        }
    }

    public static class Top10FareTipAmount extends Top10 implements Serializable {
        public static final long serialVersionUID = 1;

        public Top10FareTipAmount(CellProfitability[] top10ProfitableCells) {
            super(top10ProfitableCells);
        }
    }

    /**
     * General message to update the given Cell. All other messages inherit from this
     * message.
     */
    public static class UpdateCell implements Serializable {
        public static final long serialVersionUID = 1;
        public final Cell cell;
        public final int cellId;
        public final int tripId;

        public UpdateCell(Cell cell, int cellId, int tripId) {
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

        public IncrementEmptyTaxis(Cell cell, int cellId, int tripId) {
            super(cell, cellId, tripId);
        }
    }

    /**
     * Message to decrement the number of empty taxis for the given Cell.
     */
    public static class DecrementEmptyTaxis extends UpdateCell implements Serializable {
        public static final long serialVersionUID = 1;

        public DecrementEmptyTaxis(Cell cell, int cellId, int tripId) {
            super(cell, cellId, tripId);
        }
    }

    /**
     * Message to add the fare + tip amount to the cell.
     */
    public static class AddFareTipAmount extends UpdateCell implements Serializable {
        public static final long serialVersionUID = 1;
        public final float fareTipAmount;

        public AddFareTipAmount(Cell cell, int cellId, int tripId, float fareTipAmount) {
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

        public RemoveFareTipAmount(Cell cell, int cellId, int tripId, float fareTipAmount) {
            super(cell, cellId, tripId);
            this.fareTipAmount = fareTipAmount;
        }
    }

    // DEFINE ACTORS

    /**
     * Inner node Actor that contains top profitable cells.
     */
    public static class TopCellNode extends AbstractActor {
        // ACTOR STATE
        // top Profitable cells for a group of actors
        BoundedPriorityQueue<CellProfitability> top10ProfitableCells = new BoundedPriorityQueue<>
                (Comparator.<CellProfitability>naturalOrder(), 10);

        // Actor references to the next 10 child Actors
        ActorRef[] topCellNodes = new ActorRef[10];

        ActorRef parentNode;

        // message count of updates that we get from child actors
        int msgCountEmptyTaxis = 0;
        int msgCountFareTipAmount = 0;

        public TopCellNode() {
            // define BEHAVIOUR on message receive
            receive(ReceiveBuilder.
                    // message that contains request from parent to get current top 10 profitable cells.
                    // Respond with current top 10 cells.
                    match(GetTop10EmptyTaxis.class, message -> {
                        sender().tell(new Top10EmptyTaxis(top10ProfitableCells.toArray(
                                new CellProfitability[top10ProfitableCells.size()]
                        )), self());
                    }).

                    // message that contains request from parent to get current top 10 profitable cells.
                    // Respond with current top 10 cells.
                    match(GetTop10FareTipAmount.class, message -> {
                        sender().tell(new Top10FareTipAmount(top10ProfitableCells.toArray(
                                new CellProfitability[top10ProfitableCells.size()]
                        )), self());
                    }).

                    // message to increment the number of empty taxis
                    match(IncrementEmptyTaxis.class, message -> {
                        parentNode = sender();
                        msgCountEmptyTaxis = 0;

                        // clear top 10 routes
                        top10ProfitableCells.clear();

                        // compute the bucket location for the selected Actor
                        int bucket = (int) (message.cellId % 10);

                        // and the new id
                        int id = message.cellId / 10;

                        // check if Actor ref already exists for the computed bucket
                        if (topCellNodes[bucket] == null) {
                            // does not exist, create it
                            // the type of node depends on the value of id compare to 10
                            if (id < 10) {
                                // id less then 10, then create leaf node
                                topCellNodes[bucket] = context().actorOf(Props.create(TopCellLeafNode.class),
                                        String.valueOf(bucket));
                            } else {
                                // otherwise create normal node
                                topCellNodes[bucket] = context().actorOf(Props.create(TopCellNode.class),
                                        String.valueOf(bucket));
                            }
                        }

                        // send the message to the actor, that is contained in that bucket
                        topCellNodes[bucket].tell(new IncrementEmptyTaxis(message.cell, id, message.tripId), self());
                        msgCountEmptyTaxis++;

                        // send message to all other actors to get top routes
                        for (int i = 0; i < topCellNodes.length; i++) {
                            if (i != bucket && topCellNodes[i] != null) {
                                topCellNodes[i].tell(new GetTop10EmptyTaxis(), self());
                                msgCountEmptyTaxis++;
                            }
                        }
                    }).

                    // message to decrement the number of empty taxis
                    match(DecrementEmptyTaxis.class, message -> {
                        parentNode = sender();
                        msgCountEmptyTaxis = 0;

                        // clear top 10 routes
                        top10ProfitableCells.clear();

                        // compute the bucket location for the selected Actor
                        int bucket = (int) (message.cellId % 10);

                        // and the new id
                        int id = message.cellId / 10;

                        // send the message to the actor, that is contained in that bucket
                        topCellNodes[bucket].tell(new IncrementEmptyTaxis(message.cell, id, message.tripId), self());
                        msgCountEmptyTaxis++;

                        // send message to all other actors to get top routes
                        for (int i = 0; i < topCellNodes.length; i++) {
                            if (i != bucket && topCellNodes[i] != null) {
                                topCellNodes[i].tell(new GetTop10EmptyTaxis(), self());
                                msgCountEmptyTaxis++;
                            }
                        }
                    }).

                    // message to add fare tip amount
                    match(AddFareTipAmount.class, message -> {
                        parentNode = sender();
                        msgCountFareTipAmount = 0;

                        // clear top 10 routes
                        top10ProfitableCells.clear();

                        // compute the bucket location for the selected Actor
                        int bucket = (int) (message.cellId % 10);

                        // and the new id
                        int id = message.cellId / 10;

                        // check if Actor ref already exists for the computed bucket
                        if (topCellNodes[bucket] == null) {
                            // does not exist, create it
                            // the type of node depends on the value of id compare to 10
                            if (id < 10) {
                                // id less then 10, then create leaf node
                                topCellNodes[bucket] = context().actorOf(Props.create(TopCellLeafNode.class),
                                        String.valueOf(bucket));
                            } else {
                                // otherwise create normal node
                                topCellNodes[bucket] = context().actorOf(Props.create(TopCellNode.class),
                                        String.valueOf(bucket));
                            }
                        }

                        // send the message to the actor, that is contained in that bucket
                        topCellNodes[bucket].tell(new AddFareTipAmount(message.cell, id, message.tripId, message.fareTipAmount), self());
                        msgCountFareTipAmount++;

                        // send message to all other actors to get top routes
                        for (int i = 0; i < topCellNodes.length; i++) {
                            if (i != bucket && topCellNodes[i] != null) {
                                topCellNodes[i].tell(new GetTop10FareTipAmount(), self());
                                msgCountFareTipAmount++;
                            }
                        }
                    }).

                    // message to remove fare tip amount
                    match(RemoveFareTipAmount.class, message -> {
                        parentNode = sender();
                        msgCountFareTipAmount = 0;

                        // clear top 10 routes
                        top10ProfitableCells.clear();

                        // compute the bucket location for the selected Actor
                        int bucket = (int) (message.cellId % 10);

                        // and the new id
                        int id = message.cellId / 10;

                        // send the message to the actor, that is contained in that bucket
                        topCellNodes[bucket].tell(new RemoveFareTipAmount(message.cell, id, message.tripId, message.fareTipAmount), self());
                        msgCountFareTipAmount++;

                        // send message to all other actors to get top routes
                        for (int i = 0; i < topCellNodes.length; i++) {
                            if (i != bucket && topCellNodes[i] != null) {
                                topCellNodes[i].tell(new GetTop10FareTipAmount(), self());
                                msgCountFareTipAmount++;
                            }
                        }
                    }).

                    // receive message from child which is returning its top 10 profitable cells
                    match(Top10EmptyTaxis.class, message -> {
                        msgCountEmptyTaxis--;

                        // add to top 10 cells
                        for (CellProfitability cellProfitability : message.top10ProfitableCells) {
                            if (cellProfitability != null)
                                top10ProfitableCells.offer(cellProfitability);
                        }

                        // all updates from child actors have arrived for both the empty taxis and fare tip amount
                        if (msgCountEmptyTaxis == 0 && msgCountFareTipAmount == 0) {
                            msgCountEmptyTaxis = 0;
                            msgCountFareTipAmount = 0;

                            // send these top 10 to parent
                            parentNode.tell(new Top10EmptyTaxis(top10ProfitableCells.toArray(new CellProfitability[top10ProfitableCells.size()])), self());
                        }
                    }).

                    // receive message from child which is returning its top 10 profitable cells
                    match(Top10FareTipAmount.class, message -> {
                        msgCountFareTipAmount--;

                        // add to top 10 cells
                        for (CellProfitability cellProfitability : message.top10ProfitableCells) {
                            if (cellProfitability != null)
                                top10ProfitableCells.offer(cellProfitability);
                        }

                        // all updates from child actors have arrived for both the empty taxis and fare tip amount
                        if (msgCountEmptyTaxis == 0 && msgCountFareTipAmount == 0) {
                            msgCountEmptyTaxis = 0;
                            msgCountFareTipAmount = 0;

                            // send these top 10 to parent
                            parentNode.tell(new Top10FareTipAmount(top10ProfitableCells.toArray(new CellProfitability[top10ProfitableCells.size()])), self());
                        }
                    }).

                    build());
        }

        /**
         * Leaf Actor that contains actual Profitable cells.
         */
        public static class TopCellLeafNode extends AbstractActor {
            // cells that fall in this leaf node
            CellProfitability[] profitableCells = new CellProfitability[10];

            public TopCellLeafNode() {
                receive(ReceiveBuilder.
                        // message to increment number of empty taxis
                        match(IncrementEmptyTaxis.class, message -> {
                            // if it does not exist yet, add a new profitable cell with empty taxis set to 1
                            if (profitableCells[(int) message.cellId] == null) {
                                profitableCells[(int) message.cellId] = new
                                        CellProfitability(message.cell, message.tripId, 1, 0, 0);
                            } else {
                                // otherwise update number of empty taxis and id to the trip id
                                profitableCells[(int) message.cellId].setEmptyTaxis(profitableCells[(int) message.cellId].getEmptyTaxis() + 1);
                                profitableCells[(int) message.cellId].setProfitability(
                                        profitableCells[(int) message.cellId].getMedianProfit() /
                                        profitableCells[(int) message.cellId].getEmptyTaxis());
                                profitableCells[(int) message.cellId].setId(message.tripId);
                            }

                            // return this routes to the parent node
                            sender().tell(new Top10(profitableCells), self());
                        }).

                        // message to decrement number of empty taxis
                        match(DecrementEmptyTaxis.class, message -> {
                            // update number of empty taxis
                            profitableCells[(int) message.cellId].setEmptyTaxis(profitableCells[(int) message.cellId].getEmptyTaxis() - 1);

                            // we can get 0 number of empty taxis, so we need to check for empty taxis
                            CellProfitability[] newProfitableCells = checkForEmpty(profitableCells, (int) message.cellId);

                            // return this routes to the parent node
                            sender().tell(new Top10(newProfitableCells), self());
                        }).

                        // message to add fare + tip amount
                        match(AddFareTipAmount.class, message -> {
                            // if it does not exist yet, add a new profitable cell with current fare + tip amount
                            if (profitableCells[(int) message.cellId] == null) {
                                profitableCells[(int) message.cellId] = new
                                        CellProfitability(message.cell, message.tripId, 0, message.fareTipAmount);
                            } else {
                                // otherwise update median profit with current fare + tip amount
                                profitableCells[(int) message.cellId].getMedianProfitCell().addNumberToStream(message.fareTipAmount);
                                profitableCells[(int) message.cellId].setId(message.tripId);
                            }

                            // we can get 0 number of empty taxis, so we need to check for empty taxis
                            CellProfitability[] newProfitableCells = checkForEmpty(profitableCells, (int) message.cellId);

                            // return this routes to the parent node
                            sender().tell(new Top10(newProfitableCells), self());
                        }).

                        // message remove current fare + tip amount
                                match(RemoveFareTipAmount.class, message -> {
                            // update median profit with current fare + tip amount
                            profitableCells[(int) message.cellId].getMedianProfitCell().removeNumberFromStream(message.fareTipAmount);

                            // we can get 0 number of empty taxis, so we need to check for empty taxis
                            CellProfitability[] newProfitableCells = checkForEmpty(profitableCells, (int) message.cellId);

                            // return this routes to the parent node
                            sender().tell(new Top10(newProfitableCells), self());
                        }).
                        build());
            }

            private static CellProfitability[] checkForEmpty(CellProfitability[] profitableCells, int i) {
                CellProfitability[] newProfitableCells = new CellProfitability[profitableCells.length];
                System.arraycopy(profitableCells, 0, newProfitableCells, 0, profitableCells.length);
                if (newProfitableCells[i].getEmptyTaxis() == 0)
                    newProfitableCells[i] = null;
                return newProfitableCells;
            }
        }
    }
}
