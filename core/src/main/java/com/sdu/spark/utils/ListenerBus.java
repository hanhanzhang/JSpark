package com.sdu.spark.utils;

import com.sdu.spark.scheduler.SparkListenerEvent;

/**
 * @author hanhan.zhang
 * */
public interface ListenerBus {

    default void postToAll(SparkListenerEvent event) {
        throw new UnsupportedOperationException("");
    }

}
