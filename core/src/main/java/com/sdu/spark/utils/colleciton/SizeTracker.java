package com.sdu.spark.utils.colleciton;

import com.google.common.collect.Lists;
import com.sdu.spark.utils.SizeEstimator;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * A general interface for collections to keep track of their estimated sizes in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator is somewhat expensive (order of a few milliseconds).
 *
 * @author hanhan.zhang
 * */
public class SizeTracker {

    /**
     * 被采样对象
     * */
    private Object tracker;

    /**
     * 采样速率
     * */
    private double SAMPLE_GROWTH_RATE = 1.1;


    private LinkedList<Sample> samples = Lists.newLinkedList();

    /**
     * 每次采样内存增长量
     * */
    private double bytesPerUpdate;

    /**
     * 被采样对象已更新次数
     * */
    private int numUpdates;

    /**
     * 下次采样的更新次数
     * */
    private long nextSampleNum;

    public SizeTracker() {
        this.tracker = this;
    }

    public SizeTracker(Object tracker) {
        assert tracker != null;
        this.tracker = tracker;
        resetSamples();
    }

    public void resetSamples() {
        numUpdates = 1;
        nextSampleNum = 1;
        samples.clear();
        takeSample();
    }

    public void afterUpdate() {
        numUpdates++;
        if (numUpdates == nextSampleNum) {
            takeSample();
        }
    }

    private void takeSample() {
        samples.offer(new Sample(SizeEstimator.estimate(tracker), numUpdates));

        // 只采样最近两次对象的内存容量
        if (samples.size() > 2) {
            samples.poll();
        }

        // 计算每次采样内存占用增量
        double bytesDelta;
        if (samples.size() < 2) {
            bytesDelta = 0;
        } else {
            Sample previous = samples.getFirst();
            Sample latest = samples.getLast();
            bytesDelta = (double)(latest.size - previous.size) / (latest.numUpdates - previous.numUpdates);
        }

        bytesPerUpdate = Math.max(0, bytesDelta);
        nextSampleNum = (long) Math.ceil(SAMPLE_GROWTH_RATE * numUpdates);
    }

    public long estimateSize() {
        assert(!samples.isEmpty());
        double extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.getLast().numUpdates);
        return (long) (samples.getLast().size + extrapolatedDelta);
    }

    private class Sample implements Serializable {
        long size;
        long numUpdates;

        public Sample(long size, long numUpdates) {
            this.size = size;
            this.numUpdates = numUpdates;
        }
    }
}
