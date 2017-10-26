package com.sdu.spark.memory;

import java.io.IOException;

/**
 * @author hanhan.zhang
 * */
public class TestMemoryConsumer extends MemoryConsumer {

    public TestMemoryConsumer(TaskMemoryManager taskMemoryManager, MemoryMode mode) {
        super(taskMemoryManager, 1024L, mode);
    }

    public TestMemoryConsumer(TaskMemoryManager taskMemoryManager) {
        super(taskMemoryManager);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        long used = getUsed();
        free(used);
        return used;
    }

    void use(long size) {
        long got = taskMemoryManager.acquireExecutionMemory(size, this);
        used += got;
    }

    void free(long size) {
        used -= size;
        taskMemoryManager.releaseExecutionMemory(size, this);
    }
}
