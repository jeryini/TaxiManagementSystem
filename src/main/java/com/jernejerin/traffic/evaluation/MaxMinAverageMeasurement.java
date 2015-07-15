package com.jernejerin.traffic.evaluation;

/**
 * Contains max, min and average measurements per run.
 *
 * @param <T>
 */
public class MaxMinAverageMeasurement<T, K, L> {
    public T average;
    public K min;
    public L max;

    public MaxMinAverageMeasurement(T average, K min, L max) {
        this.average = average;
        this.min = min;
        this.max = max;
    }
}