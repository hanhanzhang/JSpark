package com.sdu.spark;

import com.sdu.spark.broadcast.BroadcastManager;
import com.sdu.spark.rpc.SparkConf;

import java.util.List;

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

    public void registerShuffle(int shuffleId, int numMaps) {
        throw new UnsupportedOperationException("");
    }

    public boolean containsShuffle(int shuffleId) {
        throw new UnsupportedOperationException("");
    }

    public int getNumAvailableOutputs(int shuffleId) {
        throw new UnsupportedOperationException("");
    }

    public List<Integer> findMissingPartitions(int shuffleId) {
        throw new UnsupportedOperationException("");
    }
}
