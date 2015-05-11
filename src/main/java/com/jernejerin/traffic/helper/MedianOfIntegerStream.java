package com.jernejerin.traffic.helper;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

// Given a stream of unsorted integers, find the median element in sorted order at any given time.
// http://www.ardendertat.com/2011/11/03/programming-interview-questions-13-median-of-integer-stream/
public class MedianOfIntegerStream {

    public Queue<Integer> minHeap;
    public Queue<Integer> maxHeap;
    public int numOfElements;

    public MedianOfIntegerStream() {
        minHeap = new PriorityQueue<Integer>();
        maxHeap = new PriorityQueue<Integer>(10, new MaxHeapComparator());
        numOfElements = 0;
    }

    public void addNumberToStream(Integer num) {
        maxHeap.add(num);
        if (numOfElements%2 == 0) {
            if (minHeap.isEmpty()) {
                numOfElements++;
                return;
            }
            else if (maxHeap.peek() > minHeap.peek()) {
                Integer maxHeapRoot = maxHeap.poll();
                Integer minHeapRoot = minHeap.poll();
                maxHeap.add(minHeapRoot);
                minHeap.add(maxHeapRoot);
            }
        } else {
            minHeap.add(maxHeap.poll());
        }
        numOfElements++;
    }

    public Double getMedian() {
        // if empty return 0
        if (numOfElements == 0)
            return new Double(0);
        if (numOfElements%2 != 0)
            return new Double(maxHeap.peek());
        else
            return (maxHeap.peek() + minHeap.peek()) / 2.0;
    }

    public void removeNumberFromStream(Integer num) {
        // if it is greater then maximum on left, then
        // the number must be on right side (minimum heap)
        if (num > maxHeap.peek()) {
            if (minHeap.remove(num)) {
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
            if (maxHeap.remove(num)) {
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

    private class MaxHeapComparator implements Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o2 - o1;
        }
    }

    public static void main(String[] args) {
        MedianOfIntegerStream streamMedian = new MedianOfIntegerStream();

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