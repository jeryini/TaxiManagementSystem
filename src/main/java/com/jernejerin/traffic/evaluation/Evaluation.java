package com.jernejerin.traffic.evaluation;

import com.aliasi.util.Tuple;
import com.jernejerin.traffic.architectures.ArchitectureBuilder;
import com.jernejerin.traffic.architectures.EDASingleThread2;

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
    public static void main(String[] args) throws InterruptedException {
        // holds the results of evaluation
        List<Tuple<Long>> results1 = new ArrayList<>();
        List<Tuple<Long>> results2 = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            EDASingleThread2 eda2 = new EDASingleThread2(new ArchitectureBuilder().
                    fileNameQuery1Output("eda2_query1_" + i + ".txt"));
            long time1 = eda2.run();


            results1.add(i, Tuple.create());
        }

    }
}
