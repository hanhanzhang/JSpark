package com.sdu.spark.memory;

/**
 * @author hanhan.zhang
 * */
public class TaskMemoryManager {

    private MemoryManager manager;
    private long taskId;

    public TaskMemoryManager(MemoryManager manager, long taskId) {
        this.manager = manager;
        this.taskId = taskId;
    }

    public long cleanUpAllAllocatedMemory() {
        return 0L;
    }
}
