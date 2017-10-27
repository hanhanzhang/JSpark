package com.sdu.spark.memory;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * {@link ExecutionMemoryPool}职责:
 *
 * 1: {@link #memoryForTask}记录每个Task分配的Execution内存量
 *
 * @author hanhan.zhang
 * */
public class ExecutionMemoryPool extends MemoryPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionMemoryPool.class);

    private MemoryMode memoryMode;
    private String poolname;

    // Task内存分配信息 ==> key = taskId, value = 分配内存数
    private Map<Long, Long> memoryForTask;

    public ExecutionMemoryPool(Object lock, MemoryMode memoryMode) {
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
        this.memoryForTask = Maps.newHashMap();
    }

    @Override
    public long memoryUsed() {
        synchronized (lock) {
            long sum = 0;
            for (Map.Entry<Long, Long> entry : memoryForTask.entrySet()) {
                sum += entry.getValue();
            }
            return sum;
        }
    }

    public long getMemoryUsageForTask(long taskAttemptId) {
        return memoryForTask.getOrDefault(taskAttemptId, 0L);
    }

    public long acquireMemory(long numBytes, long taskAttemptId) throws InterruptedException {
        return acquireMemory(numBytes, taskAttemptId, new DynamicMemoryAdjust() {
            @Override
            public void maybeGrowPool(long additionalSpaceNeeded) {
                // 不做任何处理
                LOGGER.info("StaticMemoryManager not supported dynamic adjust execution memory capacity");
            }

            @Override
            public long computeMaxPoolSize() {
                return poolSize();
            }
        });
    }

    public long acquireMemory(long numBytes, long taskAttemptId, DynamicMemoryAdjust calculate) throws InterruptedException {
        synchronized (lock) {
            assert numBytes > 0 : String.format("invalid number of bytes requested: %d", numBytes);

            // Add this task to the taskMemory map just so we can keep an accurate count of the number
            // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
            if (!memoryForTask.containsKey(taskAttemptId)) {
                memoryForTask.put(taskAttemptId, 0L);
                // This will later cause waiting tasks to wake up and check numTasks again
                lock.notifyAll();
            }

            // 对应当前N个Task, 确保每个Task分配Execution内存比例: 1/2N <= X <= 1/N
            while (true) {
                long numActiveTasks = memoryForTask.keySet().size();
                long curMem = memoryForTask.get(taskAttemptId);

                // 动态调整Execution内存
                // StaticMemoryManager不支持将Storage内存转为Execution内存, 故为空方法
                // UnifiedMemoryManager支持将Storage内存转为Execution内存, 具体实现由UnifiedMemoryManager实现
                calculate.maybeGrowPool(numBytes - memoryFree());

                // 每个Task分配最大内存(1/N * maxPoolSize)、最小内存(1/2N * poolSize)
                long maxPoolSize = calculate.computeMaxPoolSize();
                long maxMemoryPerTask = maxPoolSize / numActiveTasks;
                long minMemoryPerTask = poolSize() / (2 * numActiveTasks);

                // 若是Task已分配maxMemoryPerTask, 则需要分配内存容量0
                // 若是Task分配内存尚未达到maxMemoryPerTask, 则最大分配内存取(maxMemoryPerTask - curMem, numBytes)最小
                // 值, 保证Task最大分配内存为maxMemoryPerTask
                long maxToGrant = Math.min(numBytes, Math.max(0, maxMemoryPerTask - curMem));
                // 当前可分配的内存
                long toGrant = Math.min(maxToGrant, memoryFree());

                // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
                // if we can't give it this much now, wait for other tasks to free up memory
                // (this happens if older tasks allocated lots of memory before N grew)
                if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
                    LOGGER.info("TID {} waiting for at least 1/2N of {} pool to be free", taskAttemptId, poolname);
                    lock.wait();
                } else {
                    long oldGrant = memoryForTask.get(taskAttemptId);
                    memoryForTask.put(taskAttemptId, oldGrant + toGrant);
                    return toGrant;
                }
            }
        }
    }

    public void releaseMemory(long numBytes, long taskAttemptId) {
        synchronized (lock) {
            long curMem = memoryForTask.getOrDefault(taskAttemptId, 0L);
            long memoryToFree = 0L;
            if (curMem < numBytes) {
                LOGGER.warn("Internal error: release called on {} bytes but task only has {} bytes of memory from the {} pool",
                                numBytes, curMem, poolname);
                memoryToFree = curMem;
            } else {
                memoryToFree = numBytes;
            }
            if (memoryForTask.containsKey(taskAttemptId)) {
                long oldGrant = memoryForTask.get(taskAttemptId);
                long remainGrant = oldGrant - memoryToFree;
                if (remainGrant <= 0) {
                    memoryForTask.remove(taskAttemptId);
                } else {
                    memoryForTask.put(taskAttemptId, remainGrant);
                }
                lock.notifyAll();
            }
        }
    }


    public long releaseAllMemoryForTask(long taskAttemptId) {
        synchronized (lock) {
            long numBytesToFree = getMemoryUsageForTask(taskAttemptId);
            releaseMemory(numBytesToFree, taskAttemptId);
            return numBytesToFree;
        }
    }

    public interface DynamicMemoryAdjust {
        /**
         * @param additionalSpaceNeeded : Execution内存扩容量
         * */
        void maybeGrowPool(long additionalSpaceNeeded);
        long computeMaxPoolSize();
    }
}
