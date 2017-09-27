package com.sdu.spark.scheduler;

import com.sdu.spark.storage.BlockManagerId;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface MapStatus extends Serializable {

    BlockManagerId location();

    long getSizeForBlock(int reduceId);

}
