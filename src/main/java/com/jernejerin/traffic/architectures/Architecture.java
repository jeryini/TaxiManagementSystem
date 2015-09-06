package com.jernejerin.traffic.architectures;

import com.jernejerin.traffic.entities.CellProfitability;
import com.jernejerin.traffic.entities.RouteCount;
import com.jernejerin.traffic.entities.Trip;
import com.jernejerin.traffic.helper.PollingDriver;
import com.jernejerin.traffic.client.TaxiStream;
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

    public String getFileNameQuery1Output() {
        return fileNameQuery1Output;
    }

    public void setFileNameQuery1Output(String fileNameQuery1Output) {
        this.fileNameQuery1Output = fileNameQuery1Output;
        this.fileQuery1 = new File(this.fileNameQuery1Output);
    }

    public String getFileNameQuery2Output() {
        return fileNameQuery2Output;
    }

    public void setFileNameQuery2Output(String fileNameQuery2Output) {
        this.fileNameQuery2Output = fileNameQuery2Output;
        this.fileQuery2 = new File(this.fileNameQuery2Output);
    }

    public File getFileQuery1() {
        return fileQuery1;
    }

    public File getFileQuery2() {
        return fileQuery2;
    }

    public String getHostTCP() {
        return hostTCP;
    }

    public int getPortTCP() {
        return portTCP;
    }

    public String getHostDB() {
        return hostDB;
    }

    public int getPortDB() {
        return portDB;
    }

    public String getUserDB() {
        return userDB;
    }

    public String getPassDB() {
        return passDB;
    }

    public String getSchemaDB() {
        return schemaDB;
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
                                               LocalDateTime dropOffDateTime, long timeStart, Trip trip) {
        // compute delay now as we do not want to take in the actual processing of the result
        long delay = System.currentTimeMillis() - timeStart;

        // build content string for output
        String content = pickupDateTime.toString() + ", " + dropOffDateTime.toString() + ", ";

        // iterate over all the most frequent routes
        for (RouteCount routeCount : top10) {
            content += routeCount.getRoute().getStartCell().getEast() + "." + routeCount.getRoute().getStartCell().getSouth() +
                ", " + routeCount.getRoute().getEndCell().getEast() + "." + routeCount.getRoute().getEndCell().getSouth() +
                    " (" + routeCount.getCount() + "),";
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
     * Outputs a log to a file when top 10 cells is changed.
     *
     * @param top10 the new top 10 cells
     * @param pickupDateTime the pickup date time of the event, that changed the top 10 cells
     * @param dropOffDateTime the drop off date time of the event that change the top 10 cells
     * @param timeStart time in milliseconds when the event arrived
     */
    public void writeTop10ChangeQuery2(List<CellProfitability> top10, LocalDateTime pickupDateTime,
                                       LocalDateTime dropOffDateTime, long timeStart) {
        // compute delay now as we do not want to take in the actual processing of the result
        long delay = System.currentTimeMillis() - timeStart;

        // build content string for output
        String content = pickupDateTime.toString() + ", " + dropOffDateTime.toString() + ", ";

        // iterate over all the most profitable cells
        for (CellProfitability cellProfitability : top10) {
            content += cellProfitability.getCell().getEast() + "." + cellProfitability.getCell().getSouth() +
                ", " + cellProfitability.getEmptyTaxis() + ", " + cellProfitability.getMedianProfit() + ", " +
                cellProfitability.getProfitability() + ", ";
        }

        // add a start time, end and a delay
        content += delay + "\n";

        try (FileOutputStream fop = new FileOutputStream(this.fileQuery2, true)) {
            // write to file
            fop.write(content.getBytes());
            fop.flush();
            fop.close();
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
        }
    }

    public void writeTrips(Trip trip) {
        try (FileOutputStream fop = new FileOutputStream(this.fileQuery1, true)) {
            // write to file
            fop.write((trip.toString() + "\n").getBytes());
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
    public abstract long run() throws InterruptedException;

}