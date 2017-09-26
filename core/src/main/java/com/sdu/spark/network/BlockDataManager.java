package com.sdu.spark.network;

import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.StorageLevel;

/**
 * @author hanhan.zhang
 * */
public interface BlockDataManager {

    /**
     * Interface to get local block data. Throws an exception if the block cannot be found or
     * cannot be read successfully.
     */
    ManagedBuffer getBlockData(BlockId blockId);

    /**
     * Put the block locally, using the given storage level.
     *
     * Returns true if the block was stored and false if the put operation failed or the block
     * already existed.
     */
    boolean putBlockData(BlockId blockId, ManagedBuffer data, StorageLevel level);

    /**
     * Release locks acquired by [[putBlockData()]] and [[getBlockData()]].
     */
    void releaseLock(BlockId blockId, long taskAttemptId);

}
