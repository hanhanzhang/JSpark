package com.sdu.spark.memory;

import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.storage.BlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link UnifiedMemoryManager}职责:
 *
 * 1: 申请Storage或Execution内存时, 当可用内存不足内存分配申请时, 则会收缩Execution或Storage内存, 故:
 *
 *   MaxStorageMemory和MaxExecutionMemory的数值不是固定的, 而是动态变化的
 *
 * 2:
 *
 * 1: {@link #acquireExecutionMemory(long, long, MemoryMode)} 动态调整Execution内存
 *
 * 2: {@link #acquireStorageMemory(BlockId, long, MemoryMode)} 动态调整Storage内存
 *
 *
 * @author hanhan.zhang
 * */
public class UnifiedMemoryManager extends MemoryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedMemoryManager.class);

    private static final int RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024;

    private long maxHeapMemory;
    private long onHeapStorageRegionSize;

    public UnifiedMemoryManager(SparkConf conf, int numCores) {
        this(conf, numCores, getMaxExecutionMemory(conf), getMaxStorageMemory(conf));
    }

    public UnifiedMemoryManager(SparkConf conf, int numCores,
                                long maxHeapMemory, long onHeapStorageRegionSize) {
        super(conf, numCores, maxHeapMemory, maxHeapMemory - onHeapStorageRegionSize);
        this.maxHeapMemory = maxHeapMemory;
        this.onHeapStorageRegionSize = onHeapStorageRegionSize;
        assertInvariants();
    }

    private void assertInvariants() {
        checkArgument(onHeapExecutionMemoryPool.poolSize() + onHeapStorageMemoryPool.poolSize() == maxHeapMemory);
        checkArgument(offHeapExecutionMemoryPool.poolSize() + offHeapStorageMemoryPool.poolSize() == maxOffHeapMemory);
    }

    @Override
    public long maxOnHeapStorageMemory() {
        synchronized (this) {
            return maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed();
        }
    }

    @Override
    public long maxOffHeapStorageMemory() {
        synchronized (this) {
            return maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed();
        }
    }

    @Override
    public boolean acquireStorageMemory(BlockId blockId, long numBytes, MemoryMode memoryMode) {
        assertInvariants();
        assert numBytes >= 0;

        StorageMemoryPool storagePool;
        ExecutionMemoryPool executionPool;
        long maxMemory;

        // Storage和Execution内存可动态调整, 顾最大内存需实时计算
        switch (memoryMode) {
            case OFF_HEAP:
                storagePool = offHeapStorageMemoryPool;
                executionPool = offHeapExecutionMemoryPool;
                maxMemory = maxOnHeapStorageMemory();
                break;
            case ON_HEAP:
                storagePool = onHeapStorageMemoryPool;
                executionPool = onHeapExecutionMemoryPool;
                maxMemory = maxOnHeapStorageMemory();
                break;
            default:
                throw new IllegalArgumentException("Unsupported memory model : " + memoryMode);
        }

        // 申请内存超出最大内存, 则返回False
        if (numBytes > maxMemory) {
            // Fail fast if the block simply won't fit
            LOGGER.info("Will not store $blockId as the required space ({} bytes) exceeds our " +
                        "memory limit ({} bytes)", numBytes, maxMemory);
            return false;
        }

        // 申请Storage内存超出可用内存时, 则收缩Execution内存, 扩容Storage内存
        // 当Execution收缩内存后, Storage还不能分配足够内存时, 则此时Storage申请同StaticMemoryManager Storage内存申请
        if (numBytes > storagePool.memoryFree()) {
            // 动态调整Storage内存: 取Execution可用内存、Storage不足内存最小值
            long memoryBorrowedFromExecution = Math.min(executionPool.memoryFree(),
                                                        numBytes - storagePool.memoryFree());
            executionPool.decrementPoolSize(memoryBorrowedFromExecution);
            storagePool.incrementPoolSize(memoryBorrowedFromExecution);
        }
        return storagePool.acquireMemory(blockId, (int) numBytes);
    }

    @Override
    public boolean acquireUnrollMemory(BlockId blockId, long numBytes, MemoryMode memoryMode) {
        synchronized (this) {
            return acquireStorageMemory(blockId, numBytes, memoryMode);
        }
    }

    @Override
    public long acquireExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode) {
        SimpleDynamicMemoryAdjust memoryCalculate;
        switch (memoryMode) {
            case OFF_HEAP:
                memoryCalculate = new SimpleDynamicMemoryAdjust(
                        offHeapStorageMemoryPool,
                        offHeapExecutionMemoryPool,
                        offHeapStorageMemory,
                        maxOffHeapMemory
                );
                break;
            case ON_HEAP:
                memoryCalculate = new SimpleDynamicMemoryAdjust(
                        onHeapStorageMemoryPool,
                        onHeapExecutionMemoryPool,
                        onHeapStorageRegionSize,
                        maxHeapMemory
                );
                break;
            default:
                throw new IllegalArgumentException("Unsupported memory model : " + memoryMode);
        }

        try {
            return memoryCalculate.executionPool.acquireMemory(numBytes, taskAttemptId, memoryCalculate);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static UnifiedMemoryManager apply(SparkConf conf, int numCores) {
        long maxMemory = getMaxMemory(conf);
        return new UnifiedMemoryManager(
                conf,
                numCores,
                maxMemory,
                (long) (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5))
        );
    }

    private static long getMaxMemory(SparkConf conf) {
        long systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime().maxMemory());
        long reservedMemory = conf.getLong("spark.testing.reservedMemory",
                                            conf.contains("spark.testing") ? 0 : RESERVED_SYSTEM_MEMORY_BYTES);
        long minSystemMemory = (long) Math.ceil(reservedMemory * 1.5);
        if (systemMemory < minSystemMemory) {
            throw new IllegalArgumentException(String.format("System memory %s must " +
                    "be at least %s. Please increase heap size using the --driver-memory " +
                    "option or spark.driver.memory in Spark configuration.", systemMemory, minSystemMemory));
        }
        // SPARK-12759 Check executor memory to fail fast if memory is insufficient
        if (conf.contains("spark.executor.memory")) {
            long executorMemory = conf.getSizeAsBytes("spark.executor.memory", "0");
            if (executorMemory < minSystemMemory) {
                throw new IllegalArgumentException(String.format("Executor memory %s must be at least " +
                        "%s. Please increase executor memory using the " +
                        "--executor-memory option or spark.executor.memory in Spark configuration.", executorMemory, minSystemMemory));
            }
        }
        long usableMemory = systemMemory - reservedMemory;
        double memoryFraction = conf.getDouble("spark.memory.fraction", 0.6);
        return (long) (usableMemory * memoryFraction);
    }

    private class SimpleDynamicMemoryAdjust implements ExecutionMemoryPool.DynamicMemoryAdjust {

        StorageMemoryPool storagePool;
        ExecutionMemoryPool executionPool;
        long storageRegionSize;
        long maxMemory;

        public SimpleDynamicMemoryAdjust(StorageMemoryPool storagePool, ExecutionMemoryPool executionPool,
                                         long storageRegionSize, long maxMemory) {
            this.storagePool = storagePool;
            this.executionPool = executionPool;
            this.storageRegionSize = storageRegionSize;
            this.maxMemory = maxMemory;
        }

        /**
         * 当ExecutionPool需扩容时, 减少StoragePool内存容量
         * */
        @Override
        public void maybeGrowPool(long extraMemoryNeeded) {
            if (extraMemoryNeeded > 0) {
                // There is not enough free memory in the execution pool, so try to reclaim memory from
                // storage. We can reclaim any free memory from the storage pool. If the storage pool
                // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
                // the memory that storage has borrowed from execution.
                long memoryReclaimableFromStorage = Math.max(
                        storagePool.memoryFree(),
                        storagePool.poolSize() - this.storageRegionSize);
                if (memoryReclaimableFromStorage > 0) {
                    // Only reclaim as much space as is necessary and available:
                    long spaceToReclaim = storagePool.freeSpaceToShrinkPool(
                            Math.min(extraMemoryNeeded, memoryReclaimableFromStorage));
                    storagePool.decrementPoolSize(spaceToReclaim);
                    executionPool.incrementPoolSize(spaceToReclaim);
                }
            }
        }

        /**
         * 当StoragePool逐出Block内存空间, 分配给MemoryPool时, 重新计算ExecutionPool最大可用内存
         * */
        @Override
        public long computeMaxPoolSize() {
            return maxMemory - Math.min(storagePool.memoryUsed(), storageRegionSize);
        }

    }
}
