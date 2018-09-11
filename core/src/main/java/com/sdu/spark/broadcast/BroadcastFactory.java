package com.sdu.spark.broadcast;

import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.SecurityManager;

/**
 * @author hanhan.zhang
 * */
public interface BroadcastFactory {

    void initialize(boolean isDriver, SparkConf conf, SecurityManager securityManager);

    /**
     * Creates a new broadcast variable.
     *
     * @param value value to broadcast
     * @param isLocal whether we are in local mode (single JVM process)
     * @param id unique id representing this broadcast variable
     */
    <T> Broadcast<T> newBroadcast(T value, boolean isLocal, long id);

    void unbroadcast(long id, boolean removeFromDriver, boolean blocking);

    void stop();

}
