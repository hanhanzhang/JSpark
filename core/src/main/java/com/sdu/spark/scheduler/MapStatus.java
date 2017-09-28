package com.sdu.spark.scheduler;

import com.sdu.spark.storage.BlockManagerId;

import java.io.Serializable;

/**
 * {@link MapStatus} 记录MapTask运行状态信息
 *
 *  1: {@link #location()} 标识MapTask运行所属机器信息
 *
 *  2: {@link #getSizeForBlock(int)} 获取分区数据
 *
 * @author hanhan.zhang
 * */
public interface MapStatus extends Serializable {

    BlockManagerId location();

    long getSizeForBlock(int reduceId);

}
