package com.jernejerin.traffic.client;

import com.jernejerin.traffic.entities.Trip;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import reactor.Environment;
import reactor.io.codec.StandardCodecs;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.NetStreams;
import reactor.io.net.tcp.TcpClient;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

import java.io.*;
import java.util.Arrays;


/**
 * Created by Jernej Jerin on 21.4.2015.
 */
public class TcpTaxiClient {
    private final static int PORT = 30000;
    public static void main(String[] args) throws InterruptedException{
        // environment initialization
        Environment.initialize();

        Broadcaster<String> trips = Broadcaster.create();

        // WE NEED TO CONSUME SO THAT CLIENT SENDS DATA TO SERVER FOR THIS STREAM!
        trips.dispatchOn(Environment.cachedDispatcher())
                .consume();

        // TCP client
        TcpClient<String, String> client = NetStreams.tcpClient(
                spec -> spec
                        .codec(StandardCodecs.STRING_CODEC)
                        .connect("localhost", PORT)
                        .dispatcher(Environment.cachedDispatcher())
        );

        // subscribe to publisher, that publishes trips
        client.consumeOn(Environment.cachedDispatcher(), ch -> ch.sink(trips));

        // open connection to server
        client.open();

        // setting up CSV parser
        CsvParserSettings settings = new CsvParserSettings();

        // file uses '\n' as the line separator
        settings.getFormat().setLineSeparator("\n");

        // create a CSV parser using defined settings
        CsvParser parser = new CsvParser(settings);

        // read records one by one
        parser.beginParsing(getReader("/com/jernejerin/trips_20_days.csv"));

        String[] row;
        // read line by line
        while ((row = parser.parseNext()) != null) {
            String trip = Arrays.toString(row);

            // sink values to trips broadcaster
            trips.onNext(trip);
        }

        parser.stopParsing();
    }

    public CsvParser setupParser(String lineSeparator) {
        // setting up CSV parser
        CsvParserSettings settings = new CsvParserSettings();

        // file uses '\n' as the line separator
        settings.getFormat().setLineSeparator("\n");

        // create a CSV parser using defined settings
        return new CsvParser(settings);
    }

    /**
     * Creates a reader for a resource in the relative path
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
