package com.jernejerin.traffic.architectures;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.*;

/**
 * Unit tests for Architectures.
 *
 * @author Jernej Jerin
 */
public class ArchitectureTest extends TestCase {
    /**
     * A test for checking that serial run on the same architecture object results in the same result.
     */
    @Test
    public void testSerialRun() throws InterruptedException, IOException {
        EDASingleThread3 eda3 = new EDASingleThread3(new ArchitectureBuilder().fileNameQuery1Output("output/" +
                EDASingleThread3.class.getSimpleName() + "_query1_1.txt"));
        eda3.run();

        eda3.setFileNameQuery1Output("output/" + EDASingleThread3.class.getSimpleName() + "_query1_2.txt");
        eda3.run();

        BufferedReader eda2BuffReader1 = new BufferedReader(new FileReader("output/" + EDASingleThread3.class.getSimpleName() + "_query1_1.txt"));
        BufferedReader eda2BuffReader2 = new BufferedReader(new FileReader("output/" + EDASingleThread3.class.getSimpleName() + "_query1_2.txt"));

        String expectedLine;
        while ((expectedLine = eda2BuffReader1.readLine()) != null) {
            expectedLine = expectedLine.substring(0, expectedLine.lastIndexOf(","));
            String actualLine = eda2BuffReader2.readLine();
            actualLine = actualLine.substring(0, actualLine.lastIndexOf(","));
            assertNotNull("EDA3 run 1 had more lines then the EDA3 run 2.", actualLine);
            assertEquals(expectedLine, actualLine);
        }
        assertNull("EDA3 run 2 had more lines then the EDA3 run 1.", eda2BuffReader2.readLine());
    }

    /**
     * A test for checking that all implementations return the same result, i.e. the files
     * that contain the outputs for query1 and query2 must match.
     *
     * @throws Exception
     */
    @Test
    public void testRun() throws Exception {
        Architecture eda3 = new EDASingleThread3(new ArchitectureBuilder().fileNameQuery1Output("output/" + EDASingleThread3.class.getSimpleName() + "_query1.txt"));
        eda3.run();

        BufferedReader eda2BuffReader = new BufferedReader(new FileReader(eda3.fileNameQuery1Output));
        BufferedReader eda3BuffReader = new BufferedReader(new FileReader(eda3.fileNameQuery1Output));

        String expectedLine;
        while ((expectedLine = eda2BuffReader.readLine()) != null) {
            expectedLine = expectedLine.substring(0, expectedLine.lastIndexOf(","));
            String actualLine = eda3BuffReader.readLine();
            actualLine = actualLine.substring(0, actualLine.lastIndexOf(","));
            assertNotNull("EDA3 had more lines then the EDA2.", actualLine);
            assertEquals(expectedLine, actualLine);
        }
        assertNull("EDA2 had more lines then the EDA3.", eda3BuffReader.readLine());
    }
}