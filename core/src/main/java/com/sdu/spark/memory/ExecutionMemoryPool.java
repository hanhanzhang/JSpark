package com.sdu.spark.memory;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Task计算内存池(记录内存使用信息, Task内存分配信息)
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
        return acquireMemory(numBytes, taskAttemptId, new ExecutionMemoryCalculate() {
            @Override
            public void maybeGrowPool(long additionalSpaceNeeded) {
                // 不做任何处理
            }

            @Override
            public long computeMaxPoolSize() {
                return poolSize();
            }
        });
    }

    public long acquireMemory(long numBytes, long taskAttemptId, ExecutionMemoryCalculate calculate) throws InterruptedException {
        synchronized (lock) {
            assert numBytes > 0 : String.format("invalid number of bytes requested: %d", numBytes);

            // Add this task to the taskMemory map just so we can keep an accurate count of the number
            // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
            if (!memoryForTask.containsKey(taskAttemptId)) {
                memoryForTask.put(taskAttemptId, 0L);
                // This will later cause waiting tasks to wake up and check numTasks again
                lock.notifyAll();
            }

            // Keep looping until we're either sure that we don't want to grant this request (because this
            // task would have more than 1 / numActiveTasks of the memory) or we have enough free
            // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
            while (true) {
                long numActiveTasks = memoryForTask.keySet().size();
                long curMem = memoryForTask.get(taskAttemptId);

                // In every iteration of this loop, we should first try to reclaim any borrowed execution
                // space from storage. This is necessary because of the potential race condition where new
                // storage blocks may steal the free execution memory that this task was waiting for.
                calculate.maybeGrowPool(numBytes - memoryFree());

                // Maximum size the pool would have after potentially growing the pool.
                // This is used to compute the upper bound of how much memory each task can occupy. This
                // must take into account potential free memory as well as the amount this pool currently
                // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
                // we did not take into account space that could have been freed by evicting cached blocks.
                long maxPoolSize = calculate.computeMaxPoolSize();
                long maxMemoryPerTask = maxPoolSize / numActiveTasks;
                long minMemoryPerTask = poolSize() / (2 * numActiveTasks);

                // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
                long maxToGrant = Math.min(numBytes, Math.max(0, maxMemoryPerTask - curMem));
                // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
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

    public interface ExecutionMemoryCalculate {
        /**
         * @param additionalSpaceNeeded : Execution内存扩容量
         * */
        void maybeGrowPool(long additionalSpaceNeeded);
        long computeMaxPoolSize();
    }
}
