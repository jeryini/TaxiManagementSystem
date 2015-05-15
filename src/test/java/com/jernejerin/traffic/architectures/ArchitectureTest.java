package com.jernejerin.traffic.architectures;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.LineNumberReader;

import static org.junit.Assert.*;

/**
 * Created by Jernej on 15.5.2015.
 */
public class ArchitectureTest extends TestCase {

    /**
     * A test for checking that all implementations return the same result, i.e. the files
     * that contain the outputs for query1 and query2 must match.
     *
     * @throws Exception
     */
    @Test
    public void testRun() throws Exception {
        EDASingleThread2 eda2 = new EDASingleThread2(new ArchitectureBuilder().fileNameQuery1Output("eda2_query1.txt"));
        eda2.run();

        EDASingleThread3 eda3 = new EDASingleThread3(new ArchitectureBuilder().fileNameQuery1Output("eda3_query1.txt"));
        eda3.run();

        BufferedReader eda2BuffReader = new BufferedReader(new FileReader(eda2.fileNameQuery1Output));
        BufferedReader eda3BuffReader = new BufferedReader(new FileReader(eda3.fileNameQuery1Output));

        String expectedLine;
        while ((expectedLine = eda2BuffReader.readLine()) != null) {
            String actualLine = eda3BuffReader.readLine();
            assertNotNull("EDA3 had more lines then the EDA2.", actualLine);
            assertEquals(expectedLine, actualLine);
        }
        assertNull("EDA2 had more lines then the EDA3.", eda3BuffReader.readLine());
    }
}