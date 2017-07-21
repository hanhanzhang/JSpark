package com.sdu.spark.broadcast;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.SparkConf;

/**
 * @author hanhan.zhang
 * */
public class BroadcastManager {

    private boolean isDriver;
    private SparkConf conf;
    private SecurityManager securityManager;

    public BroadcastManager(boolean isDriver, SparkConf conf, SecurityManager securityManager) {
        this.isDriver = isDriver;
        this.conf = conf;
        this.securityManager = securityManager;
    }
}
