package com.jernejerin.traffic.evaluation;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a measurement for single architecture for the evaluation purpose.
 */
public class Measurement {
    // number of runs
    public int n;

    public String name;

    // median duration of execution from n runs
    public double medianExecutionTime;

    // median delay for query 1 from average delay per run
    public double medianDelayQuery1;

    // median delay for query 2 from average delay per run
    public double medianDelayQuery2;

    // median delay for buffer size
    public int medianBufferSize;

    // median CPU usage
    public double medianCPU;

    // median heap memory usage
    public double medianHeapMemory;

    // a list of measurements for each run
    public List<RunMeasurement> runMeasurements;

    public Measurement(int n, String name, double medianExecutionTime, double medianDelayQuery1,
                       double medianDelayQuery2, int medianBufferSize, double medianCPU, double medianHeapMemory,
                       List<RunMeasurement> runMeasurements) {
        this.n = n;
        this.name = name;
        this.medianExecutionTime = medianExecutionTime;
        this.medianDelayQuery1 = medianDelayQuery1;
        this.medianDelayQuery2 = medianDelayQuery2;
        this.medianBufferSize = medianBufferSize;
        this.medianCPU = medianCPU;
        this.medianHeapMemory = medianHeapMemory;
        this.runMeasurements = runMeasurements;
    }
}
