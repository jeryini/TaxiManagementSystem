package com.jernejerin.traffic.architectures;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for Architectures.
 *
 * @author Jernej Jerin
 */
public class ArchitectureTest extends TestCase {
    private List<Architecture> architectures;

    protected void setUp() throws Exception {
        super.setUp();

        // a list of defined architectures to test
        architectures = new ArrayList<Architecture>();

        // the first one should always be EDAPrimer as it is an example of correct solution
//        architectures.add(new EDAPrimer(new ArchitectureBuilder()));
        architectures.add(new EDA(new ArchitectureBuilder().fileNameQuery1Output("output/query/" +
                EDA.class.getSimpleName() + "_query1.txt").fileNameQuery2Output("output/query/" +
                EDA.class.getSimpleName() + "_query2.txt")));
        architectures.add(new AEDA(new ArchitectureBuilder().fileNameQuery1Output("output/query/" +
                AEDA.class.getSimpleName() + "_query1.txt").fileNameQuery2Output("output/query/" +
                AEDA.class.getSimpleName() + "_query2.txt")));
//        architectures.add(new AEDA2(new ArchitectureBuilder().fileNameQuery1Output("output/query/" +
//                AEDA2.class.getSimpleName() + "_query1.txt").fileNameQuery2Output("output/query/" +
//                AEDA2.class.getSimpleName() + "_query2.txt")));
    }

    /**
     * A test for checking that serial run on the same architecture object results in the same result.
     */
    @Test
    public void testSerialRun() throws InterruptedException, IOException {
        for (Architecture architecture : architectures) {
            architecture.setFileNameQuery1Output("output/query/" + architecture.getClass().getSimpleName() + "_query1_1.txt");
            architecture.setFileNameQuery2Output("output/query/" + architecture.getClass().getSimpleName() + "_query2_1.txt");

            // run the first time
            architecture.run();

            // set output for the second time
            architecture.setFileNameQuery1Output("output/query/" + architecture.getClass().getSimpleName() + "_query1_2.txt");
            architecture.setFileNameQuery2Output("output/query/" + architecture.getClass().getSimpleName() + "_query2_2.txt");

            // run for the second time
            architecture.run();

            // reader for generated output files
            // TODO (Jernej Jerin): Add support for query 2.
            BufferedReader edaBuffReader1 = new BufferedReader(new FileReader("output/query/" +
                    architecture.getClass().getSimpleName() + "_query1_1.txt"));
            BufferedReader edaBuffReader2 = new BufferedReader(new FileReader("output/query/" +
                    architecture.getClass().getSimpleName() + "_query1_2.txt"));

            String expectedLine;
            while ((expectedLine = edaBuffReader1.readLine()) != null) {
                expectedLine = expectedLine.substring(0, expectedLine.lastIndexOf(","));
                String actualLine = edaBuffReader2.readLine();
                actualLine = actualLine.substring(0, actualLine.lastIndexOf(","));
                assertNotNull(architecture.getClass().getSimpleName() + " run 1 had more lines then the " +
                        architecture.getClass().getSimpleName() + " run 2.", actualLine);
                assertEquals(expectedLine, actualLine);
            }
            assertNull(architecture.getClass().getSimpleName() + "run 2 had more lines then the " +
                    architecture.getClass().getSimpleName() + " run 1.", edaBuffReader2.readLine());
        }
    }

    /**
     * A test for checking that all implementations return the same result, i.e. the files
     * that contain the outputs for query1 and query2 must match. The comparison is done
     * with the output of the EDAPrimer solution.
     *
     * @throws Exception
     */
    @Test
    public void testRun() throws Exception {
        // TODO (Jernej Jerin): Add support for query 2.
        Architecture edaPrimer = architectures.get(0);
        edaPrimer.run();

        for (Architecture architecture : architectures.subList(1, architectures.size())) {
            architecture.run();

            BufferedReader edaPrimerBuffReader = new BufferedReader(new FileReader(edaPrimer.fileNameQuery1Output));
            BufferedReader architectureBuffReader = new BufferedReader(new FileReader(architecture.fileNameQuery1Output));
            String expectedLine;
            while ((expectedLine = edaPrimerBuffReader.readLine()) != null) {
                expectedLine = expectedLine.substring(0, expectedLine.lastIndexOf(","));
                String actualLine = architectureBuffReader.readLine();
                actualLine = actualLine.substring(0, actualLine.lastIndexOf(","));
                assertNotNull(edaPrimer.getClass().getSimpleName() + " had more lines then the " +
                        architecture.getClass().getSimpleName(), actualLine);
                assertEquals(expectedLine, actualLine);
            }
            assertNull(architecture.getClass().getSimpleName() + " had more lines then the " + edaPrimer.getClass().getSimpleName(),
                    architectureBuffReader.readLine());
        }
    }
}