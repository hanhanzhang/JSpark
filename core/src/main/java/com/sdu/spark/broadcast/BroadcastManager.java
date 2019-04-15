package com.sdu.spark.broadcast;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.SparkConf;
import org.apache.commons.collections.map.AbstractReferenceMap;
import org.apache.commons.collections.map.ReferenceMap;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hanhan.zhang
 * */
public class BroadcastManager {

    private boolean isDriver;
    private SparkConf conf;
    private SecurityManager securityManager;

    private BroadcastFactory broadcastFactory;

    private boolean initialized = false;
    private AtomicLong nextBroadcastId = new AtomicLong(0);

    public ReferenceMap cacheValues = new ReferenceMap(AbstractReferenceMap.HARD, AbstractReferenceMap.WEAK);

    public BroadcastManager(boolean isDriver, SparkConf conf, SecurityManager securityManager) {
        this.isDriver = isDriver;
        this.conf = conf;
        this.securityManager = securityManager;
        initialize();
    }

    private void initialize() {
        synchronized (this) {
            if (!initialized) {
                broadcastFactory = new TorrentBroadcastFactory();
                broadcastFactory.initialize(isDriver, conf, securityManager);
                initialized = true;
            }
        }
    }

    public <T> Broadcast<T> newBroadcast(T value, boolean isLocal) {
        return broadcastFactory.newBroadcast(value, isLocal, nextBroadcastId.getAndIncrement());
    }

    public void stop() {
        broadcastFactory.stop();
    }
}

