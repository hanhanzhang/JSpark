package com.sdu.spark.memory;

import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Block存储内存池, 有两个主要方法:
 *
 * 1: {@link StorageMemoryPool#acquireMemory(BlockId, long, long)}
 *
 *  申请存储空间, Storage存储空间不足时, {@link MemoryStore}会清除Block
 *
 * 2: {@link StorageMemoryPool#freeSpaceToShrinkPool(long)}
 *
 *  Execution存储内存不足时, {@link UnifiedMemoryManager}会收缩Storage存储内存
 *
 * @author hanhan.zhang
 * */
public class StorageMemoryPool extends MemoryPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageMemoryPool.class);

    private MemoryMode memoryMode;
    private String poolname;

    // 标识当前存储内存池已使用量(确保线程安全)
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
        // synchronized支持可重性
        synchronized (lock) {
            // StorageMemoryPool需要释放内存量(也就是说, 当前StorageMemoryPool可用内侧不足以满足当前Block内存需求)
            long numBytesToFree = Math.max(0, numBytes - memoryFree());
            return acquireMemory(blockId, numBytes, numBytesToFree);
        }
    }

    /**
     * @param blockId: 数据块
     * @param numBytesToAcquire: blockId申请的内存容量
     * @param numBytesToFree: 当前可用内存容量不能满足内存申请量, 内存池需释放的内存量
     * */
    public boolean acquireMemory(BlockId blockId, long numBytesToAcquire, long numBytesToFree) {
        synchronized (lock) {
            assert(numBytesToAcquire >= 0);
            assert(numBytesToFree >= 0);
            assert(memoryUsed <= poolSize());

            if (numBytesToFree > 0) {
                // 内存中Block可Spill到Disk条件:
                //  1: Block存储MemoryModel与memoryMode相同
                //  2: Block无其他进程读取(BlockInfoManager记录Block读取状态信息)
                // 调用连:
                //  MemoryStore.evictBlocksToFreeSpace()【选择可将从内存逐出到磁盘的BlockId】
                //       |
                //       +----> BlockEvictionHandler.dropFromMemory()【BlockManager是该接口唯一实现】
                //                  |
                //                  +---> MemoryStore.remove()
                //                          |
                //                          +----> MemoryManager.releaseStorageMemory()
                //                                       |
                //                                       +----> StorageMemoryPool.releaseMemory()
                memoryStore.evictBlocksToFreeSpace(blockId, numBytesToFree, memoryMode);
            }
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
                LOGGER.warn("Attempted to release {} bytes of storage memory when we only have {} bytes", size, memoryUsed);
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
