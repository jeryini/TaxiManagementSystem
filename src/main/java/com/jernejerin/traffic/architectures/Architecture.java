package com.jernejerin.traffic.architectures;

import com.jernejerin.traffic.entities.RouteCount;
import com.jernejerin.traffic.helper.PollingDriver;
import com.jernejerin.traffic.helper.TaxiStream;
import org.apache.commons.cli.*;
import reactor.Environment;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpServer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A representation of architecture for different types of architectures. For constructing
 * instances we use the Builder Design Pattern.
 *
 * <p> Each architecture uses common options and methods. The following class
 * specifies those. To see the default attribute values see the ArchitectureBuilder class.
 *
 * @author Jernej Jerin
 */
public abstract class Architecture {
    protected String hostTCP;
    protected int portTCP;
    protected String hostDB;
    protected int portDB;
    protected String userDB;
    protected String passDB;
    protected String schemaDB;
    protected String fileNameInput;
    protected String fileNameQuery1Output;
    protected String fileNameQuery2Output;
    protected TcpServer<String, String> serverTCP;
    protected boolean streamingTCP = false;
    protected TaxiStream taxiStream;
    protected Environment env;
    protected File fileQuery1;
    protected File fileQuery2;

    private final static Logger LOGGER = Logger.getLogger(Architecture.class.getName());

    public Architecture() { }

    public Architecture(ArchitectureBuilder builder) {
        // set attributes
        this.hostTCP = builder.hostTCP;
        this.portTCP = builder.portTCP;
        this.hostDB = builder.hostDB;
        this.portDB = builder.portDB;
        this.userDB = builder.userDB;
        this.passDB = builder.passDB;
        this.schemaDB = builder.schemaDB;
        this.fileNameInput = builder.fileNameInput;
        this.fileNameQuery1Output = builder.fileNameQuery1Output;
        this.fileNameQuery2Output = builder.fileNameQuery2Output;
        this.streamingTCP = builder.streamingTCP;

        // initialize the environment
        this.env = Environment.initializeIfEmpty().assignErrorJournal();

        // create the TCP server
        this.serverTCP = NetStreams.tcpServer(
            spec -> spec
                .env(this.env)
                .listen(this.hostTCP, this.portTCP)
                .dispatcher(Environment.cachedDispatcher())
                .codec(StandardCodecs.STRING_CODEC)
        );

        // create a taxi service
        this.taxiStream = new TaxiStream("/com/jernejerin/" + this.fileNameInput);

        // initializes output files given the file name
        this.fileQuery1 = new File(this.fileNameQuery1Output);
        this.fileQuery2 = new File(this.fileNameQuery2Output);

        // set up and register the PoolingDriver
        try {
            PollingDriver.setupDriver("jdbc:mysql://" + this.hostDB + ":" + this.portDB + "/" + this.schemaDB,
                    this.userDB, this.passDB);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Outputs a log to a file when top 10 routes is changed.
     *
     * @param top10 the new top 10 routes
     * @param pickupDateTime the pickup date time of the event, that changed the top 10 routes
     * @param dropOffDateTime the drop off date time of the event that change the top 10 routes
     * @param timeStart time in milliseconds when the event arrived
     */
    public void writeTop10ChangeQuery1(List<RouteCount> top10, LocalDateTime pickupDateTime,
                                               LocalDateTime dropOffDateTime, long timeStart) {
        // compute delay now as we do not want to take in the actual processing of the result
        long delay = System.currentTimeMillis() - timeStart;

        // build content string for output
        String content = pickupDateTime.toString() + ", " + dropOffDateTime.toString() + ", ";

        // iterate over all the most frequent routes
        for (RouteCount routeCount : top10) {
            content += routeCount.route.getStartCell().getEast() + "." + routeCount.route.getStartCell().getSouth() +
                ", " + routeCount.route.getEndCell().getEast() + "." + routeCount.route.getEndCell().getSouth() +
                    " (" + routeCount.count + "), ";
        }

        // add a start time, end and a delay
        content += delay + "\n";

        try (FileOutputStream fop = new FileOutputStream(this.fileQuery1, true)) {
            // write to file
            fop.write(content.getBytes());
            fop.flush();
            fop.close();
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
        }
    }

    /**
     * Outputs a log to a file when top 10 routes is changed.
     *
     * @param top10 the new top 10 routes
     * @param pickupDateTime the pickup date time of the event, that changed the top 10 routes
     * @param dropOffDateTime the drop off date time of the event that change the top 10 routes
     * @param timeStart time in milliseconds when the event arrived
     */
    public void writeExecutionTime(List<RouteCount> top10, LocalDateTime pickupDateTime,
                                       LocalDateTime dropOffDateTime, long timeStart) {
        // compute delay now as we do not want to take in the actual processing of the result
        long delay = System.currentTimeMillis() - timeStart;

        // build content string for output
        String content = pickupDateTime.toString() + ", " + dropOffDateTime.toString() + ", ";

        // iterate over all the most frequent routes
        for (RouteCount routeCount : top10) {
            content += routeCount.route.getStartCell().getEast() + "." + routeCount.route.getStartCell().getSouth() +
                    ", " + routeCount.route.getEndCell().getEast() + "." + routeCount.route.getEndCell().getSouth() +
                    " (" + routeCount.count + "), ";
        }

        // add a start time, end and a delay
        content += delay + "\n";

        try (FileOutputStream fop = new FileOutputStream(this.fileQuery1, true)) {
            // write to file
            fop.write(content.getBytes());
            fop.flush();
            fop.close();
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
        }
    }



    /**
     * Method to run implemented architecture. The method should return the
     * time to run the solution in milliseconds.
     */
    abstract long run() throws InterruptedException;

}

/**
 * A class for building the Architecture object using
 * the Builder Design Pattern.
 *
 * @author Jernej Jerin
 */
class ArchitectureBuilder {
    /** The default hostname of the TCP server. */
    protected String hostTCP = "localhost";

    /** The default port of the TCP server. */
    protected int portTCP = 30000;

    /** The default host of the DB server. */
    protected String hostDB = "localhost";

    /** The default port of the DB server. */
    protected int portDB = 3307;

    /** The default user of the DB server. */
    protected String userDB = "root";

    /** The default password of the DB server. */
    protected String passDB = "password";

    /** The default schema name to use. */
    protected String schemaDB = "taxi_trip_management";

    /** The default input file name. */
    protected String fileNameInput = "trips_example.csv";

    /** The output file name for query 1. */
    protected String fileNameQuery1Output = "query1.txt";

    /** The output file name for query 2. */
    protected String fileNameQuery2Output = "query2.txt";

    /** The default value if we are streaming from TCP client. */
    protected boolean streamingTCP = false;

    public ArchitectureBuilder() { }

    public ArchitectureBuilder hostTCP(String hostTCP) {
        this.hostTCP = hostTCP;
        return this;
    }

    public ArchitectureBuilder portTCP(int portTCP) {
        this.portTCP = portTCP;
        return this;
    }

    public ArchitectureBuilder hostDB(String hostDB) {
        this.hostDB = hostDB;
        return this;
    }

    public ArchitectureBuilder portDB(int portDB) {
        this.portDB = portDB;
        return this;
    }

    public ArchitectureBuilder userDB(String userDB) {
        this.userDB = userDB;
        return this;
    }

    public ArchitectureBuilder passDB(String passDB) {
        this.passDB = passDB;
        return this;
    }

    public ArchitectureBuilder schemaDB(String schemaDB) {
        this.schemaDB = schemaDB;
        return this;
    }

    public ArchitectureBuilder fileNameInput(String fileNameInput) {
        this.fileNameInput = fileNameInput;
        return this;
    }

    public ArchitectureBuilder fileNameQuery1Output(String fileNameQuery1Output) {
        this.fileNameQuery1Output = fileNameQuery1Output;
        return this;
    }

    public ArchitectureBuilder fileNameQuery2Output(String fileNameQuery2Output) {
        this.fileNameQuery2Output = fileNameQuery2Output;
        return this;
    }

    public ArchitectureBuilder streamingTCP(boolean streamingTCP) {
        this.streamingTCP = streamingTCP;
        return this;
    }

    /**
     * Set options from passed command line arguments. The following
     * options are set:
     *  - host TCP
     *  - port TCP
     *  - host DB
     *  - port DB
     *  - user DB
     *  - pass DB
     *  - schema DB
     *  - input file name
     *  - query1 output file name
     *  - query2 output file name
     *
     * It also prints the display help if user passes in help option.
     *
     * @param args an array of command line arguments
     */
    public void setOptionsCmd(String[] args) {
        // options for specifying command line options
        Options options = new Options();

        // add arguments
        options.addOption("help", false, "help for usage");
        options.addOption("hostTCP", true, "the hostname of the TCP server");
        options.addOption("portTCP", true, "the port of the TCP server");
        options.addOption("hostDB", true, "the hostname of the DB server");
        options.addOption("portDB", true, "the port of the DB server");
        options.addOption("userDB", true, "the username for the DB server");
        options.addOption("passDB", true, "the password for the DB server");
        options.addOption("schemaDB", true, "the schema name of the DB to use");
        options.addOption("fileNameInput", true, "the name of the input file that holds the data");
        options.addOption("fileNameQuery1Output", true, "the name of the output file to write the results for query 1");
        options.addOption("fileNameQuery2Output", true, "the name of the output file to write the results for query 1");

        // parser for command line arguments
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        // display help
        if (cmd.hasOption("help")) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("java -jar " + this.getClass().getName(), options);
            System.exit(-1);
        }

        // set values
        if (cmd.getOptionValue("hostTCP") != null)
            this.hostTCP = cmd.getOptionValue("hostTCP");
        if (cmd.getOptionValue("portTCP") != null)
            this.portTCP = Integer.parseInt(cmd.getOptionValue("portTCP"));
        if (cmd.getOptionValue("hostDB") != null)
            this.hostDB = cmd.getOptionValue("hostDB");
        if (cmd.getOptionValue("portDB") != null)
            this.portDB = Integer.parseInt(cmd.getOptionValue("portDB"));
        if (cmd.getOptionValue("userDB") != null)
            this.userDB = cmd.getOptionValue("userDB");
        if (cmd.getOptionValue("passDB") != null)
            this.passDB = cmd.getOptionValue("passDB");
        if (cmd.getOptionValue("schemaDB") != null)
            this.schemaDB = cmd.getOptionValue("schemaDB");
        if (cmd.getOptionValue("fileNameInput") != null)
            this.fileNameInput = cmd.getOptionValue("fileNameInput");
        if (cmd.getOptionValue("fileNameQuery1Output") != null)
            this.fileNameQuery1Output = cmd.getOptionValue("fileNameQuery1Output");
        if (cmd.getOptionValue("fileNameQuery2Output") != null)
            this.fileNameQuery2Output = cmd.getOptionValue("fileNameQuery2Output");
    }
}
