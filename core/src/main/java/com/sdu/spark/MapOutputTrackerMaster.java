package com.sdu.spark;

import com.sdu.spark.broadcast.BroadcastManager;
import com.sdu.spark.rpc.SparkConf;

/**
 * @author hanhan.zhang
 * */
public class MapOutputTrackerMaster extends MapOutputTracker {

    private BroadcastManager broadcastManager;
    private boolean isLocal;

    public MapOutputTrackerMaster(SparkConf conf, BroadcastManager broadcastManager, boolean isLocal) {
        super(conf);
        this.broadcastManager = broadcastManager;
        this.isLocal = isLocal;
    }

}
