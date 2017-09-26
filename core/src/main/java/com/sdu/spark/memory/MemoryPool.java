package com.sdu.spark.memory;

import com.google.common.base.Preconditions;

/**
 * JVM数据存储及数据计算存储内存池
 *
 * @author hanhan.zhang
 * */
public abstract class MemoryPool {

    protected Object lock;
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
