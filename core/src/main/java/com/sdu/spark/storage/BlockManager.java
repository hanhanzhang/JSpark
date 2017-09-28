package com.sdu.spark.storage;

import com.sdu.spark.MapOutputTracker;
import com.sdu.spark.memory.MemoryManager;
import com.sdu.spark.network.BlockDataManager;
import com.sdu.spark.network.BlockTransferService;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.serializer.SerializerManager;
import com.sdu.spark.shuffle.ShuffleManager;
import com.sdu.spark.utils.ChunkedByteBuffer;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class BlockManager implements BlockDataManager {

    private String executorId;
    private RpcEnv rpcEnv;
    private BlockManagerMaster master;
    private SerializerManager serializerManager;
    private SparkConf conf;
    private MemoryManager memoryManager;
    private MapOutputTracker mapOutputTracker;
    private ShuffleManager shuffleManager;
    private BlockTransferService blockTransferService;
    private SecurityManager securityManager;
    private int numUsableCores;

    public BlockManager(String executorId, RpcEnv rpcEnv, BlockManagerMaster master,
                        SerializerManager serializerManager, SparkConf conf, MemoryManager memoryManager,
                        MapOutputTracker mapOutputTracker, ShuffleManager shuffleManager,
                        BlockTransferService blockTransferService, SecurityManager securityManager,
                        int numUsableCores) {
        this.executorId = executorId;
        this.rpcEnv = rpcEnv;
        this.master = master;
        this.serializerManager = serializerManager;
        this.conf = conf;
        this.memoryManager = memoryManager;
        this.mapOutputTracker = mapOutputTracker;
        this.shuffleManager = shuffleManager;
        this.blockTransferService = blockTransferService;
        this.securityManager = securityManager;
        this.numUsableCores = numUsableCores;
    }


    public void initialize(String appId) {

    }

    public void reRegister() {

    }

    public List<BlockId> releaseAllLocksForTask(long taskId) {
        throw new UnsupportedOperationException("");
    }

    public boolean putBytes(BlockId blockId, ChunkedByteBuffer bytes, StorageLevel level) {
        assert bytes != null : "Bytes is null";
        return doPutBytes(blockId, bytes, level, true);
    }


    private boolean doPutBytes(BlockId blockId, ChunkedByteBuffer bytes,
                                   StorageLevel level, boolean tellMaster) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public ManagedBuffer getBlockData(BlockId blockId) {
        return null;
    }

    @Override
    public boolean putBlockData(BlockId blockId, ManagedBuffer data, StorageLevel level) {
        return false;
    }

    @Override
    public void releaseLock(BlockId blockId, long taskAttemptId) {

    }
}
