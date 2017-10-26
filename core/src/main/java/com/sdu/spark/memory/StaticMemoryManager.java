package com.sdu.spark.memory;

import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.storage.BlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * 不支持动态调整Storage和Execution内存
 *
 * @author hanhan.zhang
 * */
public class StaticMemoryManager extends MemoryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticMemoryManager.class);

    // Max number of bytes worth of blocks to evict when unrolling
    private long maxUnrollMemory;

    public StaticMemoryManager(SparkConf conf, int numCores) {
        this(conf, numCores, getMaxStorageMemory(conf), getMaxExecutionMemory(conf));
    }

    public StaticMemoryManager(SparkConf conf, int numCores,
                               long onHeapStorageMemory, long onHeapExecutionMemory) {
        super(conf, numCores, onHeapStorageMemory, onHeapExecutionMemory);

        this.maxUnrollMemory = (long) (onHeapStorageMemory * conf.getDouble("spark.storage.unrollFraction", 0.2));

        // StaticMemoryManager不支持OFF_HEAP
        offHeapExecutionMemoryPool.incrementPoolSize(offHeapStorageMemoryPool.poolSize());
        offHeapStorageMemoryPool.decrementPoolSize(offHeapStorageMemoryPool.poolSize());
    }

    @Override
    public long maxOnHeapStorageMemory() {
        return onHeapStorageMemory;
    }

    @Override
    public long maxOffHeapStorageMemory() {
        return 0L;
    }

    @Override
    public boolean acquireStorageMemory(BlockId blockId, long numBytes, MemoryMode memoryMode) {
        checkArgument(memoryMode != MemoryMode.OFF_HEAP,
                        "StaticMemoryManager does not support off-heap storage memory");
        if (numBytes > maxOnHeapStorageMemory()) {
            // Fail fast if the block simply won't fit
            LOGGER.info("Will not store {} as the required space ({} bytes) exceeds our " +
                        "memory limit ({} bytes)", blockId, numBytes, maxOffHeapStorageMemory());
            return false;
        } else {
            return onHeapStorageMemoryPool.acquireMemory(blockId, (int) numBytes);
        }
    }

    @Override
    public boolean acquireUnrollMemory(BlockId blockId, long numBytes, MemoryMode memoryMode) {
        checkArgument(memoryMode != MemoryMode.OFF_HEAP,
                        "StaticMemoryManager does not support off-heap unroll memory");
        long currentUnrollMemory = onHeapStorageMemoryPool.memoryStore().currentUnrollMemory();
        long freeMemory = onHeapStorageMemoryPool.memoryFree();
        // When unrolling, we will use all of the existing free memory, and, if necessary,
        // some extra space freed from evicting cached blocks. We must place a cap on the
        // amount of memory to be evicted by unrolling, however, otherwise unrolling one
        // big block can blow away the entire cache.
        long maxNumBytesToFree = Math.max(0, maxUnrollMemory - currentUnrollMemory - freeMemory);
        // Keep it within the range 0 <= X <= maxNumBytesToFree
        long numBytesToFree = Math.max(0, Math.min(maxNumBytesToFree, numBytes - freeMemory));
        return onHeapStorageMemoryPool.acquireMemory(blockId, numBytes, numBytesToFree);
    }

    @Override
    public long acquireExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode) {
        try {
            switch (memoryMode) {
                case OFF_HEAP:
                    return offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId);
                case ON_HEAP:
                    return onHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId);
                default:
                    throw new IllegalArgumentException("Unsupported memory mode : " + memoryMode);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
