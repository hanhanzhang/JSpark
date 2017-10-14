package com.sdu.spark.rdd;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public abstract class RDDCheckpointData<T> implements Serializable {

    public transient RDD<T> rdd;

    public synchronized boolean isCheckpointed() {
        // TODO: 待实现
        throw new UnsupportedOperationException("");
    }
}
