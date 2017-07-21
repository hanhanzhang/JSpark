package com.sdu.spark;

import com.sdu.spark.rpc.SparkConf;

/**
 * @author hanhan.zhang
 * */
public abstract class MapOutputTracker {

    /**
     *
     * */
    protected long epoch;
    protected Object epochLock = new Object();

    protected SparkConf conf;

    public MapOutputTracker(SparkConf conf) {
        this.conf = conf;
    }

}
