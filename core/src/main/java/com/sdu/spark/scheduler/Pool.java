package com.sdu.spark.scheduler;

/**
 * @author hanhan.zhang
 * */
public class Pool implements Schedulable {
    private String poolName;
    private SchedulingMode schedulingMode;
    private int initMinShare;
    private int initWeight;

    public Pool(String poolName, SchedulingMode schedulingMode,
                int initMinShare, int initWeight) {
        this.poolName = poolName;
        this.schedulingMode = schedulingMode;
        this.initMinShare = initMinShare;
        this.initWeight = initWeight;
    }

    @Override
    public void addSchedulable(Schedulable schedulable) {
        throw new UnsupportedOperationException("");
    }
}
