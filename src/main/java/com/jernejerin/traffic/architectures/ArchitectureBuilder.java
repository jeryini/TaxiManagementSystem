package com.jernejerin.traffic.architectures;

import org.apache.commons.cli.*;

/**
 * A class for building the Architecture object using
 * the Builder Design Pattern.
 *
 * @author Jernej Jerin
 */
public class ArchitectureBuilder {
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
    protected String fileNameInput = "trips_1_hour_2013-01-01-00-00_2013-01-01-01-00_10799.csv";

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
            help.printHelp("java -jar FileName", options);
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
