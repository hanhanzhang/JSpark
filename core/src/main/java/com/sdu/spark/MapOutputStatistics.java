package com.sdu.spark;

/**
 * @author hanhan.zhang
 * */
public class MapOutputStatistics {

    public int shuffleId;

    public long[] bytesByPartitionId;

    public MapOutputStatistics(int shuffleId, long[] bytesByPartitionId) {
        this.shuffleId = shuffleId;
        this.bytesByPartitionId = bytesByPartitionId;
    }
}
