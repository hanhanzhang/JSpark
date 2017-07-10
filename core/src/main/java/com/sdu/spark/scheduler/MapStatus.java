package com.sdu.spark.scheduler;

import com.sdu.spark.storage.BlockManagerId;

/**
 * @author hanhan.zhang
 * */
public interface MapStatus {

    BlockManagerId location();

    long getSizeForBlock(int reduceId);

}
