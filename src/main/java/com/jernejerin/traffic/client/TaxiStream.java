package com.jernejerin.traffic.client;

import com.jernejerin.traffic.entities.Trip;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.reactivestreams.Processor;
import reactor.Environment;
import reactor.core.processor.RingBufferProcessor;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

/**
 * A taxi stream class for reading data and broadcasting it to the
 * different architectures. It can be used for streaming from TCP client
 * to TCP server or from local.
 *  
 * @author Jernej Jerin
 */
public class TaxiStream {
    private final static Logger LOGGER = Logger.getLogger(TaxiStream.class.getName());
    private String fileName;

    // for asynchronous broadcasting the Processor is the recommended way
    // instead of using the Broadcaster
    private Processor<String, String> tripsProcessor;
    private Stream<String> trips;

    public TaxiStream(String fileName) {
        this.fileName = fileName;

        // create a Processor with an internal RingBuffer capacity of 32 slots
        this.tripsProcessor = RingBufferProcessor.create("trips", 32);

        // create a Reactor Stream from this Reactive Streams Processor
        this.trips = Streams.wrap(this.tripsProcessor);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Stream<String> getTrips() {
        return trips;
    }

    public void setTrips(Broadcaster<String> trips) {
        this.trips = trips;
    }

    /**
     * Read stream of taxi trip data from file
     * and broadcast its value next.
     *
     * @throws InterruptedException
     */
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
            this.tripsProcessor.onNext(trip);
        }
        // close the channel as we are finished streaming data
        // this sends a complete signal which we can in turn observe
        this.tripsProcessor.onComplete();

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
