package com.sdu.spark.memory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link MemoryPool}职责:
 *
 * 1: 管理内存使用信息
 *
 *   1': {@link #poolSize}                      ==>> 内存池容量
 *
 *   2': {@link #memoryUsed()}                  ==>> 内存池已使用量
 *
 *   3': {@link #incrementPoolSize(long)}       ==>> 内存池扩容
 *
 *   4': {@link #decrementPoolSize(long)}       ==>> 内存池缩容
 *
 * 2: MemoryPool有两种实现:
 *
 *  1': {@link ExecutionMemoryPool}             ==> Task计算内存
 *
 *  2': {@link StorageMemoryPool}               ==> Block存储内存
 *
 * 3: 内存池申请和释放需确保线程安全({@link #lock}确保单线程访问)
 *
 * 4: {@link MemoryManager}负责对{@link #poolSize}初始化
 *
 * @author hanhan.zhang
 * */
public abstract class MemoryPool {

    protected final Object lock;
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
            checkArgument(delta >= 0, "memory pool increment number should greater than zero");
            poolSize += delta;
        }
    }

    public final void decrementPoolSize(long delta) {
        synchronized (lock) {
            checkArgument(delta >= 0, "memory pool decrement number should greater than zero");
            checkArgument(delta <= poolSize, String.format("memory pool decrement number should less than pool size %d", poolSize));
            checkArgument(poolSize - delta >= memoryUsed(), String.format("memory pool decrement number should less than free space %d", poolSize - delta));
            poolSize -= delta;
        }
    }


    public abstract long memoryUsed();
}
