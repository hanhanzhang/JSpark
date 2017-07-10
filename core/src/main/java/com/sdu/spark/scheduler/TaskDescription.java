package com.sdu.spark.scheduler;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class TaskDescription {

    public long taskId;
    public int attemptNumber;
    public String executorId;
    public String name;
    public int index;    // Index withIn this task's TaskSet
    public Map<String, Long> addedFiles;
    public Map<String, Long> addedJars;
    public Properties properties;
    public ByteBuffer serializedTask;

    public TaskDescription(long taskId, int attemptNumber, String executorId, String name, int index,
                           Map<String, Long> addedFiles, Map<String, Long> addedJars, Properties properties,
                           ByteBuffer serializedTask) {
        this.taskId = taskId;
        this.attemptNumber = attemptNumber;
        this.executorId = executorId;
        this.name = name;
        this.index = index;
        this.addedFiles = addedFiles;
        this.addedJars = addedJars;
        this.properties = properties;
        this.serializedTask = serializedTask;
    }

    @Override
    public String toString() {
        return String.format("TaskDescription(TID=%d, index=%d)", taskId, index);
    }
}
