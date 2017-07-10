package com.sdu.spark.storage;

/**
 * @author hanhan.zhang
 * */
public class TaskResultBlockId extends BlockId {

    public long taskId;

    public TaskResultBlockId(long taskId) {
        this.taskId = taskId;
    }

    @Override
    public String name() {
        return String.format("taskResult_%s", taskId);
    }
}
