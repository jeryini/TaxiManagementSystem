package com.jernejerin.traffic.helper;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Computes median value of the elements currently in the collection.
 * Class also supports removing element. The type must extend the abstract
 * class Number.
 *
 * @param <T> type of the class must extend the Number class
 */
public class MedianOfStream<T extends Number & Comparable<? super T>> {

    public Queue<T> minHeap;
    public Queue<T> maxHeap;
    public int numOfElements;

    public MedianOfStream() {
        minHeap = new PriorityQueue<>();
        maxHeap = new PriorityQueue<>(10, new MaxHeapComparator());
        numOfElements = 0;
    }

    public MedianOfStream(T n) {
        this();
        addNumberToStream(n);
    }

    public void addNumberToStream(T n) {
        maxHeap.add(n);
        if (numOfElements % 2 == 0) {
            if (minHeap.isEmpty()) {
                numOfElements++;
                return;
            }
            else if (maxHeap.peek().compareTo(minHeap.peek()) > 0) {
                T maxHeapRoot = maxHeap.poll();
                T minHeapRoot = minHeap.poll();
                maxHeap.add(minHeapRoot);
                minHeap.add(maxHeapRoot);
            }
        } else {
            minHeap.add(maxHeap.poll());
        }
        numOfElements++;
    }

    public double getMedian() {
        // if empty return 0
        if (numOfElements == 0)
            throw new NoSuchElementException();
        if (numOfElements % 2 != 0)
            return maxHeap.peek().doubleValue();
        else
            return (maxHeap.peek().doubleValue() + minHeap.peek().doubleValue()) / 2.0;
    }

    public void removeNumberFromStream(T n) {
        // if it is greater then maximum on left, then
        // the number must be on right side (minimum heap)
        if (n.compareTo(maxHeap.peek()) > 0) {
            if (minHeap.remove(n)) {
                numOfElements--;

                // if the number of elements is not odd
                // after delete, then we need to move
                // element from max heap to min
                if (numOfElements % 2 == 0) {
                    minHeap.add(maxHeap.poll());
                }
            }
        }
        else {
            if (maxHeap.remove(n)) {
                numOfElements--;

                // if the number of elements is not even
                // after delete, then we need to move
                // element from min heap to max
                if (numOfElements % 2 != 0) {
                    maxHeap.add(minHeap.poll());
                }
            }
        }
    }

    private class MaxHeapComparator implements Comparator<T> {
        @Override
        public int compare(T n1, T n2) {
            return n2.compareTo(n1);
        }
    }

    public static void main(String[] args) {
        MedianOfStream<Integer> streamMedian = new MedianOfStream();

        streamMedian.addNumberToStream(1);
        System.out.println(streamMedian.getMedian()); // should be 1

        streamMedian.addNumberToStream(5);
        streamMedian.addNumberToStream(10);
        streamMedian.addNumberToStream(12);
        streamMedian.addNumberToStream(2);
        System.out.println(streamMedian.getMedian()); // should be 5

        streamMedian.addNumberToStream(3);
        streamMedian.addNumberToStream(8);
        streamMedian.addNumberToStream(9);
        System.out.println(streamMedian.getMedian()); // should be 6.5

        streamMedian.removeNumberFromStream(9);
        System.out.println(streamMedian.getMedian()); // should be 5

        streamMedian.removeNumberFromStream(8);
        streamMedian.removeNumberFromStream(3);
        System.out.println(streamMedian.getMedian()); // should be 5

    }
}