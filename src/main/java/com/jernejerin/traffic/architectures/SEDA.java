package com.jernejerin.traffic.architectures;

import com.jernejerin.traffic.entities.Cell;
import com.jernejerin.traffic.entities.Route;
import com.jernejerin.traffic.helper.TripOperations;
import reactor.Environment;
import reactor.core.DispatcherSupplier;
import reactor.fn.tuple.Tuple;
import reactor.rx.Streams;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * An example of single threaded event driven architecture - EDA.
 * </p>
 *
 * @author Jernej Jerin
 */
public class SEDA extends Architecture {
    // initial table size is 1e5
    private static Map<Route, Route> routes = new LinkedHashMap<>(100000);

    private static Map<Cell, Cell> cells = new LinkedHashMap<>(100000);

    // current top 10 sorted
    private static LinkedList<Route> top10PreviousQuery1 = new LinkedList<>();
    private static LinkedList<Cell> top10PreviousQuery2 = new LinkedList<>();

    /** The default number of threads for stage 1. */
    private static int stage1T = 20;

    /** The default number of threads for stage 2. */
    private static int stage2T = 20;

    static int id = 0;

    DispatcherSupplier supplierStage1;
    DispatcherSupplier supplierStage2;

    private final static Logger LOGGER = Logger.getLogger(SEDA.class.getName());

    public SEDA(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
        // suppliers for stages
        this.supplierStage1 = Environment.newCachedDispatchers(stage1T, "stage1");
        this.supplierStage2 = Environment.newCachedDispatchers(stage2T, "stage2");
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting SEDA solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder();

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the SEDA solution using the builder
        SEDA seda = new SEDA(builder);

        // run the solution
        seda.run();
    }

    public long run() throws InterruptedException {
        // consumer for TCP server
        this.serverTCP.start(ch -> {
            ch.log("conn").consume(trip -> {
                LOGGER.log(Level.INFO, "TCP server receiving trip " +
                        trip + " from thread = " + Thread.currentThread());
                // dispatch event to a broadcaster pipeline
                this.taxiStream.getTrips().onNext(trip);
                LOGGER.log(Level.INFO, "TCP server send ticket to streaming pipeline for ticket = " +
                        trip + " from thread = " + Thread.currentThread());
            });
            return Streams.never();
        }).await();

        // processing through stages of streams, where each stage has a separate thread pool
        // for processing streams. We basically fork the stream in each stage to number of streams,
        // which equals the number of threads in pool
        taxiStream.getTrips()
            // dispatcher for funneling/joining result back to single thread
            .dispatchOn(Environment.sharedDispatcher())
            .map(t -> {
                // create a tuple of string trip and current time for computing delay
                // As this is our entry point it is appropriate to start the time here,
                // before any parsing is being done. This also in record with the Grand
                // challenge recommendation
                //                LOGGER.log(Level.INFO, "Distributing for ticket = " +
//                        t + " from thread = " + Thread.currentThread());
                return Tuple.of(t, System.currentTimeMillis(), id++);
            })
            // parallelize stream tasks to separate streams for stage 1 - PARSING AND SAVING DATA
            .partition(stage1T)
            // we receive streams grouped by accordingly to the positive modulo of the
            // current hashcode with respect to the number of buckets specified
            .flatMap(stream -> stream
                // dispatch on separate stream
                .dispatchOn(supplierStage1.get())
                // stage 1 is for validating ticket structure and saving it into the DB
                .map(t -> TripOperations.parseValidateTrip(t.getT1(), t.getT2(), t.getT3()))
                // group by trip validation
                .groupBy(t -> t != null)
                .map(tripValidStream -> {
                    // if trip is valid continue with operations on the trip
                    // The insert trip operation falls under the STAGE 1.
                    if (tripValidStream.key()) {
                        return tripValidStream
                                .map(t -> {
                                    // save ticket
//                            TripOperations.insertTrip(t);
                                    return t;
                                });
                    } else {
                        tripValidStream.consume(trip ->
                            LOGGER.log(Level.WARNING, "Invalid trip passed in!")
                        );
                    }
                    return Streams.<Object>empty();
                })
            )
            .dispatchOn(Environment.sharedDispatcher())
            .consume(t -> {
                t.consume();
            });

                    // partition stream for stage 2
//                        .partition(stage2T)
//                        .flatMap(stream -> stream
//                                        // dispatcher on supplier for stage 2
//                                        .dispatchOn(supplierStage2.get())
//                                                // TODO (Jernej Jerin): get current statistics for vehicle
//                                        .map(bt -> bt)
//                                                // TODO (Jernej Jerin): compute new statistics. We would like to
//                                                // TODO (Jernej Jerin): have some heavy CPU operation here to simulate
//                                                // TODO (Jernej Jerin): CPU intensive work.
//                                        .map(bt -> bt)
//                        )
//                        .dispatchOn(Environment.sharedDispatcher())
//                        .consume();
//
        // read the stream from file: for local testing
        taxiStream.readStream();

        // run the server forever
        // TODO(Jernej Jerin): Is there a better way to do this?
        Thread.sleep(Long.MAX_VALUE);

        return 0;
    }
}
