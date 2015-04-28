package com.jernejerin.traffic.client;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import org.apache.commons.cli.*;

import reactor.Environment;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpClient;
import reactor.rx.broadcast.Broadcaster;

import java.io.*;
import java.util.Arrays;


/**
 * Created by Jernej Jerin on 21.4.2015.
 */
public class TcpTaxiClient {
    /** The default hostname of the TCP server. */
    private static String hostTCP = "localhost";

    /** The default port of the TCP server. */
    private static int portTCP = 30000;

    public static void main(String[] args) throws InterruptedException{
        // set host and port from command line options
        setOptionsCmd(args);

        // environment initialization. This creates and assigns a context environment
        // bound to the current classloader.
        Environment.initialize();

        // create a broadcaster we can sink values into
        Broadcaster<String> trips = Broadcaster.create();

        // dispatch onto a new dispatcher that is suitable for streams.
        // Instruct the stream to request the produced subscription indefinitely.
        // By consuming values we are creating a demand to the TCP server
        trips.dispatchOn(Environment.cachedDispatcher())
            .consume();

        // TCP client for producing demand to the TCP server
        TcpClient<String, String> client = NetStreams.tcpClient(
            spec -> spec
                .codec(StandardCodecs.STRING_CODEC)
                .connect(hostTCP, portTCP)
                .dispatcher(Environment.cachedDispatcher())
        );

        // subscribe to trip publisher, which will reply data on the active connection
        // We basically have a stream of data from broadcaster that we now publish
        // it to the TCP server.
        client.consumeOn(Environment.cachedDispatcher(), ch -> ch.sink(trips));

        // open connection to server
        client.open();

        // setting up CSV parser
        CsvParser parser = setupParser();

        // read records one by one
        parser.beginParsing(getReader("/com/jernejerin/trips_20_days.csv"));

        String[] row;
        // read line by line
        while ((row = parser.parseNext()) != null) {
            String trip = Arrays.toString(row);

            // sink values to trips broadcaster
            trips.onNext(trip);
        }

        // finished parsing all the data from the csv file
        parser.stopParsing();

        // close client
        client.close();
    }

    /**
     * Setup a parser (line separator, etc.).
     *
     * @return a new instance using defined settings
     */
    public static CsvParser setupParser() {
        // setting up CSV parser
        CsvParserSettings settings = new CsvParserSettings();

        // file uses '\n' as the line separator
        settings.getFormat().setLineSeparator("\n");

        // create a CSV parser using defined settings
        return new CsvParser(settings);
    }

    /**
     * Set options from passed command line arguments. The following
     * options are set:
     *  - host TCP
     *  - port TCP
     * It also prints the display help if user passes in help option.
     *
     * @param args an array of command line arguments
     */
    public static void setOptionsCmd(String[] args) {
        // options for specifying command line options
        Options options = new Options();

        // add command line arguments
        options.addOption("help", false, "help for usage");
        options.addOption("hostTCP", true, "the hostname of the TCP server");
        options.addOption("portTCP", true, "the port of the TCP server");

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
            help.printHelp("java -jar TcpTaxiClient", options);
            System.exit(-1);
        }

        // set values
        if (cmd.getOptionValue("hostTCP") != null)
            hostTCP = cmd.getOptionValue("hostTCP");
        if (cmd.getOptionValue("portTCP") != null)
            portTCP = Integer.parseInt(cmd.getOptionValue("portTCP"));
    }

    /**
     * Creates a reader for a resource in the relative path
     *
     * @param relativePath relative path of the resource to be read
     * @return a reader of the resource
     */
    public static Reader getReader(String relativePath) {
        try {
            return new InputStreamReader(TcpTaxiClient.class.getResourceAsStream(relativePath), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Unable to read input", e);
        }
    }
}
