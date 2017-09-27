package com.sdu.spark;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface Partitioner extends Serializable {

    int numPartitions();
    int getPartition(Object key);

}
