package com.sdu.spark.memory;

import com.google.common.base.Preconditions;

/**
 * {@link MemoryPool}维护内存使用信息, MemoryPool有两种实现:
 *
 * 1: {@link ExecutionMemoryPool}  ==> Task计算内存
 *
 * 2: {@link StorageMemoryPool}    ==> Block存储内存
 *
 * @author hanhan.zhang
 * */
public abstract class MemoryPool {

    final Object lock;
    // 内存池容量
    private long poolSize;

    public MemoryPool(Object lock) {
        this.lock = lock;
    }

    public final long poolSize() {
        synchronized (lock) {
            return poolSize;
        }
    }

    public final long memoryFree() {
        synchronized (lock) {
            return poolSize - memoryUsed();
        }
    }

    public final void incrementPoolSize(long delta) {
        synchronized (lock) {
            Preconditions.checkArgument(delta >= 0, "memory pool increment number should greater than zero");
            poolSize += delta;
        }
    }

    public final void decrementPoolSize(long delta) {
        synchronized (lock) {
            Preconditions.checkArgument(delta >= 0, "memory pool decrement number should greater than zero");
            Preconditions.checkArgument(delta <= poolSize, String.format("memory pool decrement number should less than pool size %d", poolSize));
            Preconditions.checkArgument(poolSize - delta >= memoryUsed(), String.format("memory pool decrement number should less than free space %d", poolSize - delta));
            poolSize -= delta;
        }
    }


    public abstract long memoryUsed();
}
