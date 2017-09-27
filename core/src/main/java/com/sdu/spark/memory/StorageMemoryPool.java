package com.sdu.spark.memory;

import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用来缓存Task数据、在Spark集群中传输内部数据
 *
 * @author hanhan.zhang
 * */
public class StorageMemoryPool extends MemoryPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageMemoryPool.class);

    private MemoryMode memoryMode;
    private String poolname;

    private long memoryUsed = 0L;
    private MemoryStore memoryStore;

    public StorageMemoryPool(Object lock, MemoryMode memoryMode) {
        super(lock);

        switch (memoryMode) {
            case OFF_HEAP:
                poolname = "off-heap storage";
                break;
            case ON_HEAP:
                poolname = "on-heap storage";
                break;
            default:
                poolname = "";
        }
        this.memoryMode = memoryMode;
    }

    @Override
    public long memoryUsed() {
        synchronized (lock) {
            return memoryUsed;
        }
    }

    public void setMemoryStore(MemoryStore memoryStore) {
        this.memoryStore = memoryStore;
    }

    public MemoryStore memoryStore() {
        if (memoryStore == null) {
            throw new IllegalStateException("memory store not initialized yet");
        }
        return memoryStore;
    }

    public boolean acquireMemory(BlockId blockId, int numBytes) {
        synchronized (lock) {
            long numBytesToFree = Math.max(0, numBytes - memoryFree());
            return acquireMemory(blockId, numBytes, numBytesToFree);
        }
    }

    public boolean acquireMemory(BlockId blockId, long numBytesToAcquire, long numBytesToFree) {
        synchronized (lock) {
            assert(numBytesToAcquire >= 0);
            assert(numBytesToFree >= 0);
            assert(memoryUsed <= poolSize());

            if (numBytesToFree > 0) {
                // 没有足够的JVM内存, 需清空内存[内存清除后, MemoryPool被告知且修改可用内存]
                memoryStore.evictBlocksToFreeSpace(blockId, numBytesToFree, memoryMode);
            }

            // NOTE: If the memory store evicts blocks, then those evictions will synchronously call
            // back into this StorageMemoryPool in order to free memory. Therefore, these variables
            // should have been updated.
            boolean enoughMemory = numBytesToAcquire <= memoryFree();
            if (enoughMemory) {
                memoryUsed += numBytesToAcquire;
            }
            return enoughMemory;
        }
    }

    public void releaseMemory(long size) {
        synchronized (lock) {
            if (size > memoryUsed) {
                LOGGER.warn("Attempted to release $size bytes of storage memory when we only have {} bytes", memoryUsed);
                memoryUsed = 0;
            } else {
                memoryUsed -= size;
            }
        }
    }

    public void releaseAllMemory() {
        synchronized (lock) {
            memoryUsed = 0;
        }
    }

    /**
     * 收缩内存池容量
     *
     * @return 返回可收缩的内存量
     */
    public long freeSpaceToShrinkPool(long spaceToFree) {
        synchronized (lock) {
            // 计算可收缩内存容量
            long spaceFreedByReleasingUnusedMemory = Math.min(spaceToFree, memoryFree());
            // 收缩内存后可用内存量
            long remainingSpaceToFree = memoryFree() - spaceFreedByReleasingUnusedMemory;
            if (remainingSpaceToFree > 0) {
                long spaceFreedByEviction = memoryStore.evictBlocksToFreeSpace(null, remainingSpaceToFree, memoryMode);
                // When a block is released, BlockManager.dropFromMemory() calls releaseMemory(), so we do
                // not need to decrement _memoryUsed here. However, we do need to decrement the pool size.
                return spaceFreedByReleasingUnusedMemory + spaceFreedByEviction;
            }
            return spaceFreedByReleasingUnusedMemory;
        }
    }
}
