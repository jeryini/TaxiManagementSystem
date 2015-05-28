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
        Architecture eda = new EDA(new ArchitectureBuilder().fileNameQuery1Output("output/" +
                EDA.class.getSimpleName() + "_query1_1.txt"));
        eda.run();

        eda.setFileNameQuery1Output("output/" + EDAPrimer.class.getSimpleName() + "_query1_2.txt");
        eda.run();

        BufferedReader edaBuffReader1 = new BufferedReader(new FileReader("output/" + EDA.class.getSimpleName() + "_query1_1.txt"));
        BufferedReader edaBuffReader2 = new BufferedReader(new FileReader("output/" + EDA.class.getSimpleName() + "_query1_2.txt"));

        String expectedLine;
        while ((expectedLine = edaBuffReader1.readLine()) != null) {
            expectedLine = expectedLine.substring(0, expectedLine.lastIndexOf(","));
            String actualLine = edaBuffReader2.readLine();
            actualLine = actualLine.substring(0, actualLine.lastIndexOf(","));
            assertNotNull("EDA run 1 had more lines then the EDA run 2.", actualLine);
            assertEquals(expectedLine, actualLine);
        }
        assertNull("EDA run 2 had more lines then the EDA run 1.", edaBuffReader2.readLine());
    }

    /**
     * A test for checking that all implementations return the same result, i.e. the files
     * that contain the outputs for query1 and query2 must match.
     *
     * @throws Exception
     */
    @Test
    public void testRun() throws Exception {
        Architecture eda1 = new EDAPrimer(new ArchitectureBuilder().
//                fileNameInput("trips_1_day_2013-01-01_2013-01-02_209276.csv").
                fileNameInput("trips_1_hour_2013-01-01-00-00_2013-01-01-01-00_10799.csv").
                fileNameQuery1Output("output/" + EDAPrimer.class.getSimpleName() + "_query1.txt"));
        eda1.run();

        Architecture eda2 = new AEDA(new ArchitectureBuilder().
//                fileNameInput("trips_1_day_2013-01-01_2013-01-02_209276.csv").
                fileNameInput("trips_1_hour_2013-01-01-00-00_2013-01-01-01-00_10799.csv").
                fileNameQuery1Output("output/" + AEDA.class.getSimpleName() + "_query1.txt"));
        eda2.run();

        BufferedReader eda1BuffReader = new BufferedReader(new FileReader(eda1.fileNameQuery1Output));
        BufferedReader eda2BuffReader = new BufferedReader(new FileReader(eda2.fileNameQuery1Output));

        String expectedLine;
        while ((expectedLine = eda1BuffReader.readLine()) != null) {
            expectedLine = expectedLine.substring(0, expectedLine.lastIndexOf(","));
            String actualLine = eda2BuffReader.readLine();
            actualLine = actualLine.substring(0, actualLine.lastIndexOf(","));
            assertNotNull(eda1.getClass().getSimpleName() + " had more lines then the " +
                    eda2.getClass().getSimpleName(), actualLine);
            assertEquals(expectedLine, actualLine);
        }
        assertNull(eda2.getClass().getSimpleName() + "had more lines then the " + eda1.getClass().getSimpleName(),
                eda2BuffReader.readLine());
    }
}