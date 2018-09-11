package com.sdu.spark.broadcast;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.SparkConf;

/**
 * @author hanhan.zhang
 * */
public class TorrentBroadcastFactory implements BroadcastFactory {

    @Override
    public void initialize(boolean isDriver, SparkConf conf, SecurityManager securityManager) {

    }

    @Override
    public <T> Broadcast<T> newBroadcast(T value, boolean isLocal, long id) {
        return new TorrentBroadcast<>(id, value);
    }

    @Override
    public void unbroadcast(long id, boolean removeFromDriver, boolean blocking) {
        TorrentBroadcast.unpersist(id, removeFromDriver, blocking);
    }

    @Override
    public void stop() {

    }
}
