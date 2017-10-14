package com.sdu.spark;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public abstract class Partitioner implements Serializable {

    public abstract int numPartitions();
    public abstract int getPartition(Object key);

}
