package com.sdu.spark.memory;

import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.storage.BlockId;

/**
 * @author hanhan.zhang
 * */
public class UnifiedMemoryManager extends MemoryManager {

    public UnifiedMemoryManager(SparkConf conf, int numCores,
                                long maxHeapMemory, long onHeapStorageRegionSize) {
        super(conf, numCores, maxHeapMemory, maxHeapMemory - onHeapStorageRegionSize);
    }

    @Override
    public long maxOnHeapStorageMemory() {
        return 0;
    }

    @Override
    public long maxOffHeapStorageMemory() {
        return 0;
    }

    @Override
    public boolean acquireStorageMemory(BlockId blockId, long numBytes, MemoryMode memoryMode) {
        return false;
    }

    @Override
    public boolean acquireUnrollMemory(BlockId blockId, long numBytes, MemoryMode memoryMode) {
        return false;
    }

    @Override
    public long acquireExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode) {
        return 0;
    }
}
