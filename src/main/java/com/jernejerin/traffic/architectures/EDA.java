package com.jernejerin.traffic.architectures;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * An example of a single threaded event driven architecture - EDA.
 * This solution consists of LinkedHashMap.
 *
 * @author Jernej Jerin
 */
public class EDA extends Architecture {
    private final static Logger LOGGER = Logger.getLogger(EDASingleThread2.class.getName());

    public EDA(ArchitectureBuilder builder) {
        // call super constructor to initialize fields from builder
        super(builder);
    }

    public static void main(String[] args) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting single threaded EDA solution from thread = " + Thread.currentThread());

        // create Architecture builder
        ArchitectureBuilder builder = new ArchitectureBuilder();

        // set host and port from command line options
        builder.setOptionsCmd(args);

        // construct the EDA solution using the builder
        EDA eda = new EDA(builder);

        // run the solution
        eda.run();
    }

    public long run() throws InterruptedException {
        return 0;
    }
}
