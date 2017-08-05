package com.sdu.spark.scheduler;

import com.sdu.spark.TaskContext;
import com.sdu.spark.TaskContextImpl;
import com.sdu.spark.memory.TaskMemoryManager;
import org.apache.logging.log4j.util.Strings;

import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public abstract class Task<T> {
    private int stageId;
    private int stageAttemptId;
    private int partitionId;
    public transient Properties localProperties = new Properties();
    private int jobId;
    public String appId;
    private String appAttemptId;

    public TaskMemoryManager taskMemoryManager;

    // TaskSetManager设置
    public long epoch;

    public transient TaskContextImpl context;

    private transient Thread taskThread;

    public long executorDeserializeTime = 0L;
    public long executorDeserializeCpuTime = 0L;

    public Task(int stageId, int stageAttemptId, int partitionId, Properties localProperties,
                int jobId, String appId, String appAttemptId) {
        this.stageId = stageId;
        this.stageAttemptId = stageAttemptId;
        this.partitionId = partitionId;
        this.localProperties = localProperties;
        this.jobId = jobId;
        this.appId = appId;
        this.appAttemptId = appAttemptId;
    }

    public T run(long taskAttemptId, int attemptNumber) {
        context =  new TaskContextImpl(stageId, partitionId, taskAttemptId,
                                       attemptNumber, taskMemoryManager, localProperties);
        taskThread = Thread.currentThread();
        try {
            return runTask(context);
        } catch (Throwable e) {
            context.markTaskFailed(e);
            throw e;
        } finally {
            try {
                context.markTaskCompleted();
            } finally {
                // TODO: 释放Task的内存
            }
        }
    }

    private void kill(boolean interruptThread, String reason) {
        if (Strings.isNotEmpty(reason)) {
            if (context != null) {
                context.markInterrupted(reason);
            }
            if (interruptThread && taskThread != null) {
                taskThread.interrupt();
            }
        }
    }

    public abstract T runTask(TaskContext context);
}
