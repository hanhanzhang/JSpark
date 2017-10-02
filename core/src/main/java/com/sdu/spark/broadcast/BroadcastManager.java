package com.sdu.spark.broadcast;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.SparkConf;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hanhan.zhang
 * */
public class BroadcastManager {

    private boolean isDriver;
    private SparkConf conf;
    private SecurityManager securityManager;

    private BroadcastFactory broadcastFactory;

    private AtomicLong nextBroadcastId = new AtomicLong(0);

    public BroadcastManager(boolean isDriver, SparkConf conf, SecurityManager securityManager) {
        this.isDriver = isDriver;
        this.conf = conf;
        this.securityManager = securityManager;
    }

    public <T> Broadcast<T> newBroadcast(T value, boolean isLocal) {
        return broadcastFactory.newBroadcast(value, isLocal, nextBroadcastId.getAndIncrement());
    }

    public void stop() {

    }
}

