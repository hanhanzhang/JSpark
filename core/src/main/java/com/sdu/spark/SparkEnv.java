package com.sdu.spark;

import com.sdu.spark.memory.MemoryManager;
import com.sdu.spark.scheduler.LiveListenerBus;
import com.sdu.spark.scheduler.OutputCommitCoordinator;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;

/**
 * @author hanhan.zhang
 * */
public class SparkEnv {

    public static volatile SparkEnv env;

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

    public static SparkEnv createExecutorEnv(SparkConf conf,
                                             String executorId,
                                             String hostname,
                                             int numCores,
                                             byte[] ioEncryptionKey,
                                             boolean isLocal) {
        SparkEnv env = create(conf, executorId, hostname, hostname, 0, isLocal,
                              numCores, ioEncryptionKey, null, null);
        SparkEnv.env = env;
        return env;
    }

    private static SparkEnv create(SparkConf conf,
                                   String executorId,
                                   String bindAddress,
                                   String advertiseAddress,
                                   int port,
                                   boolean isLocal,
                                   int numUsableCores,
                                   byte[] ioEncryptionKey,
                                   LiveListenerBus liveListenerBus,
                                   OutputCommitCoordinator mockOutputCommitCoordinator) {
        throw new UnsupportedOperationException("");
    }

}
