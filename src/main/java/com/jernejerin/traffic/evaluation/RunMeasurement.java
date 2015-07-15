package com.jernejerin.traffic.evaluation;

import java.util.Locale;

/**
 * Contains all measurements per run.
 *
 */
public class RunMeasurement {
    public int id;
    public long executionTime;
    public MaxMinAverageMeasurement<Double, Long, Long> query1;
    public MaxMinAverageMeasurement<Double, Long, Long> query2;
    public MaxMinAverageMeasurement<Double, Long, Long> bufferSize;
    public MaxMinAverageMeasurement<Double, Long, Long> cpu;
    public MaxMinAverageMeasurement<Double, Long, Long> heapMemory;

    public RunMeasurement(int id, long executionTime, MaxMinAverageMeasurement<Double, Long, Long> query1,
                          MaxMinAverageMeasurement<Double, Long, Long> query2, MaxMinAverageMeasurement<Double, Long, Long> bufferSize,
                          MaxMinAverageMeasurement<Double, Long, Long> cpu, MaxMinAverageMeasurement<Double, Long, Long> heapMemory) {
        this.id = id;
        this.executionTime = executionTime;
        this.query1 = query1;
        this.query2 = query2;
        this.bufferSize = bufferSize;
        this.cpu = cpu;
        this.heapMemory = heapMemory;
    }
}