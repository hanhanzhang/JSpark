package com.sdu.spark.memory;

import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.memory.MemoryStore;
import com.sdu.spark.unfase.Platform;
import com.sdu.spark.unfase.array.ByteArrayMethods;
import com.sdu.spark.unfase.memory.MemoryAllocator;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link MemoryManager}负责Execution和Storage内存申请、及Execution内存动态扩容
 *
 * todo: 操作系统内存页管理
 *
 * @author hanhan.zhang
 * */
public abstract class MemoryManager {

    public SparkConf conf;
    int numCores;
    // Storage内存存储量
    long onHeapStorageMemory;
    // Execution计算内存存储量
    long onHeapExecutionMemory;

    // 最大非堆内存存储量
    long maxOffHeapMemory;
    // 非堆Storage内存存储量
    long offHeapStorageMemory;

    final StorageMemoryPool onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP);
    final StorageMemoryPool offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP);
    final ExecutionMemoryPool onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP);
    final ExecutionMemoryPool offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP);

    MemoryMode tungstenMemoryMode;
    long pageSizeBytes;
    // 分配分配
    MemoryAllocator tungstenMemoryAllocator;

    public MemoryManager(SparkConf conf, int numCores, long onHeapStorageMemory, long onHeapExecutionMemory) {
        this.conf = conf;
        this.numCores = numCores;
        this.onHeapStorageMemory = onHeapStorageMemory;
        this.onHeapExecutionMemory = onHeapExecutionMemory;

        // 初始化jvm内存容量
        this.onHeapStorageMemoryPool.incrementPoolSize(this.onHeapStorageMemory);
        this.onHeapExecutionMemoryPool.incrementPoolSize(this.onHeapExecutionMemory);

        // 初始化DirectMemory容量
        this.maxOffHeapMemory = conf.getSizeAsBytes("spark.memory.offHeap.size", "0");
        this.offHeapStorageMemory = (long) (maxOffHeapMemory * conf.getDouble("spark.memory.storageFraction", 0.5));
        this.offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory);
        this.offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory);

        // 内存模型
        this.tungstenMemoryMode = memoryMode(conf);
        // 内存页计算
        this.pageSizeBytes = calculatePageSize(conf);
        // 内存分配
        this.tungstenMemoryAllocator = memoryAllocator(this.tungstenMemoryMode);
    }

    private MemoryMode memoryMode(SparkConf conf) {
        if (conf.getBoolean("spark.memory.offHeap.enabled", false)) {
            checkArgument(conf.getSizeAsBytes("spark.memory.offHeap.size", "0") > 0,
                    "spark.memory.offHeap.size must be > 0 when spark.memory.offHeap.enabled == true");

            checkArgument(Platform.unaligned(),
                    "No support for unaligned Unsafe. Set spark.memory.offHeap.enabled to false.");
            return MemoryMode.OFF_HEAP;
        }
        return MemoryMode.ON_HEAP;
    }

    private long calculatePageSize(SparkConf conf) {
        long minPageSize = 1024 * 1024L;   // 1MB
        long maxPageSize = 64 * minPageSize;  // 64MB
        int cores = numCores > 0 ? numCores : Runtime.getRuntime().availableProcessors();
        // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
        int safetyFactor = 16;
        long maxTungstenMemory = 0L;
        switch (tungstenMemoryMode) {
            case OFF_HEAP:
                maxTungstenMemory = onHeapExecutionMemoryPool.poolSize();
                break;
            case ON_HEAP:
                maxTungstenMemory = offHeapExecutionMemoryPool.poolSize();
                break;
        }
        long size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor);
        long defaultPageSize = Math.min(maxPageSize, Math.max(minPageSize, size));
        return conf.getSizeAsBytes("spark.buffer.pageSize", String.valueOf(defaultPageSize));
    }

    private MemoryAllocator memoryAllocator(MemoryMode memoryMode) {
        switch (memoryMode) {
            case OFF_HEAP:
                return MemoryAllocator.UNSAFE;
            case ON_HEAP:
                return MemoryAllocator.HEAP;
            default:
                throw new UnsupportedOperationException("Unsupported memory model : " + memoryMode);
        }
    }

    public final void setMemoryStore(MemoryStore memoryStore) {
        onHeapStorageMemoryPool.setMemoryStore(memoryStore);
        offHeapStorageMemoryPool.setMemoryStore(memoryStore);
    }

    /**
     * Total available off heap memory for storage, in bytes. This amount can vary over time,
     * depending on the MemoryManager implementation.
     *
     * Note:
     *  {@link UnifiedMemoryManager} Storage Memory amount can vary over time
     * */
    public abstract long maxOnHeapStorageMemory();
    public abstract long maxOffHeapStorageMemory();

    public abstract boolean acquireStorageMemory(BlockId blockId, long numBytes, MemoryMode memoryMode);
    public abstract boolean acquireUnrollMemory(BlockId blockId, long numBytes, MemoryMode memoryMode);
    public synchronized void releaseStorageMemory(long numBytes, MemoryMode memoryMode) {
        switch (memoryMode) {
            case OFF_HEAP:
                offHeapStorageMemoryPool.releaseMemory(numBytes);
                break;
            case ON_HEAP:
                onHeapStorageMemoryPool.releaseMemory(numBytes);
                break;
        }
    }
    public synchronized void releaseAllStorageMemory() {
        onHeapStorageMemoryPool.releaseAllMemory();
        offHeapStorageMemoryPool.releaseAllMemory();
    }
    public final void releaseUnrollMemory(long numBytes, MemoryMode memoryMode) {
        releaseStorageMemory(numBytes, memoryMode);
    }
    public final synchronized long storageMemoryUsed() {
        return onHeapStorageMemoryPool.memoryUsed() + offHeapStorageMemoryPool.memoryUsed();
    }



    public abstract long acquireExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode);
    public void releaseExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode) {
        switch (memoryMode) {
            case OFF_HEAP:
                offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId);
                break;
            case ON_HEAP:
                onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId);
                break;
        }
    }
    public synchronized long releaseAllExecutionMemoryForTask(long taskAttemptId) {
        return onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
                offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId);
    }
    public final synchronized long executionMemoryUsed() {
        return onHeapExecutionMemoryPool.memoryUsed() + offHeapExecutionMemoryPool.memoryUsed();
    }
    /**
     * Returns the execution memory consumption, in bytes, for the given task.
     */
    public synchronized long getExecutionMemoryUsageForTask(long taskAttemptId) {
        return onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
                offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId);
    }

}
