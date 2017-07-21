package com.sdu.spark;

import com.sdu.spark.memory.MemoryManager;
import com.sdu.spark.scheduler.LiveListenerBus;
import com.sdu.spark.scheduler.OutputCommitCoordinator;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;

import static com.sdu.spark.security.CryptoStreamUtils.createKey;

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

    public static SparkEnv createDriverEnv(SparkConf conf,
                                           boolean isLocal,
                                           LiveListenerBus listenerBus,
                                           int numCores,
                                           OutputCommitCoordinator mockOutputCommitCoordinator) {
        assert conf.contains("spark.driver.host") : "Spark Driver host is not set !";
        String bindAddress = conf.get("spark.driver.bindAddress");
        String advertiseAddress = conf.get("spark.driver.host");
        int port = NumberUtils.toInt(conf.get("spark.driver.port"));
        byte[] ioEncryptionKey = null;
        boolean isEncryption = BooleanUtils.toBoolean(conf.get("spark.io.encryption.enabled"));
        if (isEncryption) {
            ioEncryptionKey = createKey(conf);
        }

        return create(
                conf,
                "driver",
                bindAddress,
                advertiseAddress,
                port,
                isLocal,
                numCores,
                ioEncryptionKey,
                listenerBus,
                mockOutputCommitCoordinator);
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
