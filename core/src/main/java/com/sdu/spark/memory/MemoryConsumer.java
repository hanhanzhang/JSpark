package com.sdu.spark.memory;

import com.sdu.spark.unfase.array.LongArray;
import com.sdu.spark.unfase.memory.MemoryBlock;

import java.io.IOException;

/**
 * @author hanhan.zhang
 * */
public abstract class MemoryConsumer {

    protected TaskMemoryManager taskMemoryManager;
    private long pageSize;
    private MemoryMode mode;
    protected long used;

    protected MemoryConsumer(TaskMemoryManager taskMemoryManager,
                             long pageSize,
                             MemoryMode mode) {
        this.taskMemoryManager = taskMemoryManager;
        this.pageSize = pageSize;
        this.mode = mode;
    }

    protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
        this(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
    }

    public MemoryMode getMode() {
        return mode;
    }

    protected long getUsed() {
        return used;
    }

    public void spill() throws IOException {
        spill(Long.MAX_VALUE, this);
    }

    public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

    public LongArray allocateArray(long size) {
        long required = size * 8L;
        MemoryBlock page = taskMemoryManager.allocatePage(required, this);
        if (page == null || page.size() < required) {
            long got = 0;
            if (page != null) {
                // 已分配内存容量小于申请的内存容量
                got = page.size();
                taskMemoryManager.freePage(page, this);
            }
            taskMemoryManager.showMemoryUsage();
            throw new OutOfMemoryError(String.format("申请内存容量%d bytes失败，实际可分配内存容量%d bytes", required, got));
        }
        used += required;
        return new LongArray(page);
    }

    protected MemoryBlock allocatePage(long required) {
        MemoryBlock memoryBlock = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
        if (memoryBlock == null) {
            throw new OutOfMemoryError(String.format("申请内存容量%d bytes失败", required));
        } else if (memoryBlock.size() < required){
            long got = memoryBlock.size();
            taskMemoryManager.freePage(memoryBlock, this);
            taskMemoryManager.showMemoryUsage();
            throw new OutOfMemoryError(String.format("申请内存容量%d bytes失败，实际可分配内存容量%d bytes", required, got));
        }
        used += memoryBlock.size();
        return memoryBlock;
    }

    public void freeArray(LongArray array) {
        freePage(array.memoryBlock());
    }

    protected void freePage(MemoryBlock page) {
        used -= page.size();
        taskMemoryManager.freePage(page, this);
    }

    public long acquireMemory(long size) {
        long granted = taskMemoryManager.acquireExecutionMemory(size, this);
        used += granted;
        return granted;
    }

    /**
     * Release N bytes of memory.
     */
    public void freeMemory(long size) {
        taskMemoryManager.releaseExecutionMemory(size, this);
        used -= size;
    }



}
