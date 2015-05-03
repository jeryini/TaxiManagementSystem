package com.jernejerin.traffic.helper;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import reactor.Environment;
import reactor.rx.broadcast.Broadcaster;

import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

/**
 * <p>
 * A taxi stream class for reading data and broadcasting it to the
 * different architectures. It can be used for streaming from TCP client
 * to TCP server or from local.
 * </p>
 *  
 * @author Jernej Jerin
 */
public class TaxiStream {
    private final static Logger LOGGER = Logger.getLogger(TaxiStream.class.getName());
    private String fileName;
    // we use a hot stream as we have a unbounded stream of data incoming.
    private Broadcaster<String> trips;

    public TaxiStream(String fileName) {
        this.fileName = fileName;
        this.trips = Broadcaster.create(Environment.get());
        // dispatch onto a new dispatcher that is suitable for streams
        this.trips.dispatchOn(Environment.cachedDispatcher());
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Broadcaster<String> getTrips() {
        return trips;
    }

    public void setTrips(Broadcaster<String> trips) {
        this.trips = trips;
    }

    public void readStream() throws InterruptedException {
        // setting up CSV parser
        CsvParser parser = setupParser();

        // read records one by one
        parser.beginParsing(getReader(this.fileName));

        String[] row;

        // read line by line
        while ((row = parser.parseNext()) != null) {
            // create a string from array separated by comma
            String trip = String.join(",", row);

            // sink values to trips broadcaster
            this.trips.onNext(trip);

            // need to sleep because it is to fast :)
            Thread.sleep(10);
        }

        // finished parsing all the data from the csv file
        parser.stopParsing();
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
     * Creates a reader for a resource in the relative path
     *
     * @param relativePath relative path of the resource to be read
     * @return a reader of the resource
     */
    public static Reader getReader(String relativePath) {
        try {
            return new InputStreamReader(TaxiStream.class.getResourceAsStream(relativePath), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Unable to read input", e);
        }
    }
}
