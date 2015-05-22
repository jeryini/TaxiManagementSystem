package com.jernejerin.traffic.evaluation;

import com.jernejerin.traffic.architectures.Architecture;
import com.jernejerin.traffic.architectures.ArchitectureBuilder;
import com.jernejerin.traffic.architectures.EDAPrimer;
import com.jernejerin.traffic.helper.MedianOfStream;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.fn.tuple.Tuple3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains evaluation of different architectures. The evaluation is done on the
 * following attributes:
 * <ul>
 *     <li> delay - the delay for writing each top 10. This is the average value of delays.
 *     <li> total duration of execution - The total time it took to complete.
 *
 * @author Jernej Jerin
 */
public class Evaluation {
    public static void main(String[] args) throws InterruptedException, IOException {
        Architecture edaPrimer = new EDAPrimer(new ArchitectureBuilder());
        Tuple3<Double, Tuple2<Double, Double>, List<Tuple3<Long, Double, Double>>> result2 = evaluate(edaPrimer, 10);
        System.out.println("EDAPrimer:");
        printResult(result2);
    }

    /**
     * Evaluate the passed architecture multiple times. Before the evaluation, run the
     * solution to cache values. The subsequent results should therefore not be impacted as much
     * by cache misses.
     *
     * @param architecture the architecture to evaluate
     * @param numTimes how many number of times to evaluate the solution
     * @return a triple, where the first element is median duration of execution, the
     * second element is a tuple of median values for the average delay per run (query1 and query2),
     * and the third is a list of duration of execution and average delay for each run
     * @throws InterruptedException
     * @throws IOException
     */
    public static Tuple3<Double, Tuple2<Double, Double>, List<Tuple3<Long, Double, Double>>> evaluate(Architecture architecture,
            int numTimes) throws InterruptedException, IOException {
        MedianOfStream<Long> medianDuration = new MedianOfStream<>();
        MedianOfStream<Double> medianDelayQuery1 = new MedianOfStream<>();
        MedianOfStream<Double> medianDelayQuery2 = new MedianOfStream<>();

        // holds the result for each evaluation
        List<Tuple3<Long, Double, Double>> results = new ArrayList<>();

        //  run once before taking measurements to avoid taking into account cache misses
        architecture.setFileNameQuery1Output("output/" + architecture.getClass().getSimpleName() + "_query1_cache.txt");
        architecture.setFileNameQuery2Output("output/" + architecture.getClass().getSimpleName() + "_query2_cache.txt");
        architecture.run();

        for (int i = 0; i < numTimes; i++) {
            architecture.setFileNameQuery1Output("output/" + architecture.getClass().getSimpleName() + "_query1_" + i + ".txt");
            architecture.setFileNameQuery2Output("output/" + architecture.getClass().getSimpleName() + "_query2_" + i + ".txt");

            long duration = architecture.run();
            double averageDelayQuery1 = getAverage(architecture.getFileQuery1().toPath());
//            double averageDelayQuery2 = getAverage(architecture.getFileQuery2().toPath());

            // save the duration to median
            medianDuration.addNumberToStream(duration);

            // compute the average delay and save it to median
            medianDelayQuery1.addNumberToStream(averageDelayQuery1);
//            medianDelayQuery2.addNumberToStream(averageDelayQuery2);

            results.add(i, Tuple.of(duration, averageDelayQuery1, 0d));
        }
        return Tuple.of(medianDuration.getMedian(), Tuple.of(medianDelayQuery1.getMedian(),
                0d), results);
    }

    /**
     * Get the average of the delay values in output files for query 1 and 2.
     *
     * @param filePath the file path of the file user wants to parse
     * @return the average of delays
     * @throws IOException
     */
    public static double getAverage(Path filePath) throws IOException {
        return Files.lines(filePath)
                .map(line -> line.split(","))
                .mapToLong(splits -> Long.parseLong(splits[splits.length - 1]))
                .average().getAsDouble();
    }

    /**
     * Output formatted results. First three lines are median values of multiple runs.
     * Then come lines consisting of result for execution time and average delay per each run.
     *
     * @param result the result to output
     */
    public static void printResult(Tuple3<Double, Tuple2<Double, Double>, List<Tuple3<Long, Double, Double>>> result) {
        System.out.format("%40s%n", "Median duration of execution: " + result.getT1() + " ms");
        System.out.format("%40s%n", "Median of the average delay for query 1: " + result.getT2().getT1() + " ms");
        System.out.format("%40s%n%n", "Median of the average delay for query 2: " + result.getT2().getT2() + " ms");

        System.out.format("%30s%30s%30s%n", "Duration of execution", "Average delay for query 1", "Average delay for query 2");
        for (Tuple3<Long, Double, Double> run : result.getT3()) {
            System.out.format("%30d%30f%30f%n", run.getT1(), run.getT2(), run.getT3());
        }
    }
}
