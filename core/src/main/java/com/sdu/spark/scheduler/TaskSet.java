package com.sdu.spark.scheduler;

import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class TaskSet {

    public Task<?>[] tasks;
    public int stageId;
    public int stageAttemptId;
    public int priority;
    public Properties properties;

    public TaskSet(Task<?>[] tasks, int stageId, int stageAttemptId, int priority, Properties properties) {
        this.tasks = tasks;
        this.stageId = stageId;
        this.stageAttemptId = stageAttemptId;
        this.priority = priority;
        this.properties = properties;
    }

    public String id() {
        return String.format("%s.%s", stageId, stageAttemptId);
    }


}
