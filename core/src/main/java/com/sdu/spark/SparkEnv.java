package com.sdu.spark;

import com.sdu.spark.memory.MemoryManager;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;

/**
 * @author hanhan.zhang
 * */
public class SparkEnv {
    public RpcEnv rpcEnv;
    public SparkConf conf;
    public BlockManager blockManager;
    public MemoryManager memoryManager;
    public Serializer serializer;
    public MapOutputTracker mapOutputTracker;


    public SparkEnv(RpcEnv rpcEnv, SparkConf conf, BlockManager blockManager, MemoryManager memoryManager,
                    MapOutputTracker mapOutputTracker, Serializer serializer) {
        this.rpcEnv = rpcEnv;
        this.conf = conf;
        this.blockManager = blockManager;
        this.memoryManager = memoryManager;
        this.mapOutputTracker = mapOutputTracker;
        this.serializer = serializer;
    }

    public void stop() {

    }
}
