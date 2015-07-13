package com.jernejerin.traffic.client;

import org.apache.commons.cli.*;

import reactor.Environment;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpClient;
import reactor.rx.Stream;

/**
 * <p>
 * A running program for reading Taxi data from local
 * file and sending it to the TCP server. The program parses
 * the file line by line. It accepts the following arguments:
 * - hostTCP - TCP server host name
 * - portTCP - TCP server port
 * - fileName - Name of the file
 *
 * For more info, use --help.
 * </p>
 *
 * @author Jernej Jerin
 */
public class TcpTaxiClient {
    /** The default hostname of the TCP server. */
    private static String hostTCP = "localhost";

    /** The default port of the TCP server. */
    private static int portTCP = 30000;

    /** The default file name. */
    private static String fileName = "trips_example.csv";

    public static void main(String[] args) throws InterruptedException{
        // set host and port from command line options
        setOptionsCmd(args);

        // environment initialization. This creates and assigns a context environment
        // bound to the current class loader.
        Environment.initialize();

        // create a broadcaster we can sink values into
        TaxiStream taxiStream = new TaxiStream("/com/jernejerin/" + fileName);

        // instruct the stream to request the produced subscription indefinitely
        // by consuming values we are creating a demand to the TCP server
        taxiStream.getTrips().consume();

        // a separate sink stream with capacity of one
        Stream<String> sink = taxiStream.getTrips().log("trips").capacity(1L);

        // TCP client for producing demand to the TCP server
        TcpClient<String, String> client = NetStreams.tcpClient(
            spec -> spec
                .codec(StandardCodecs.STRING_CODEC)
                .connect(hostTCP, portTCP)
                .dispatcher(Environment.cachedDispatcher())
        );

        // start a connection to server. Subscribe to sink publisher, which will reply data on
        // the active connection. We basically have a stream of data from broadcaster that we now publish
        // it to the TCP server.
        client.start(ch -> ch.writeWith(sink));

        // read stream of data
        taxiStream.readStream();

        // shutdown client
        client.shutdown().await();
    }

    /**
     * <p>
     * Set options from passed command line arguments. The following
     * options are set:
     *  - host TCP
     *  - port TCP
     *  - file name
     * It also prints the display help if user passes in help option.
     * </p>
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
        options.addOption("fileName", true, "the name of the file that holds the data");

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
        if (cmd.getOptionValue("fileName") != null)
            fileName = cmd.getOptionValue("fileName");
    }

}
