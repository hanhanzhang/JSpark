package com.sdu.spark.scheduler;

import com.sdu.spark.SparkEnv;
import com.sdu.spark.SparkException;
import com.sdu.spark.TaskContext;
import com.sdu.spark.TaskContextImpl;
import com.sdu.spark.memory.MemoryMode;
import com.sdu.spark.memory.TaskMemoryManager;
import org.apache.logging.log4j.util.Strings;

import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public abstract class Task<T> {
    protected int stageId;
    public int stageAttemptId;
    protected int partitionId;
    public transient Properties localProperties = new Properties();
    private int jobId;
    public String appId;
    private String appAttemptId;

    private TaskMemoryManager taskMemoryManager;

    // TaskSetManager设置
    public long epoch;

    public transient TaskContextImpl context;
    private transient volatile String reasonIfKilled = null;
    // 运行Task的线程
    private transient Thread taskThread;

    public long executorDeserializeTime = 0L;
    public long executorDeserializeCpuTime = 0L;

    public Task(int stageId,
                int stageAttemptId,
                int partitionId,
                Properties localProperties,
                int jobId,
                String appId,
                String appAttemptId) {
        this.stageId = stageId;
        this.stageAttemptId = stageAttemptId;
        this.partitionId = partitionId;
        this.localProperties = localProperties;
        this.jobId = jobId;
        this.appId = appId;
        this.appAttemptId = appAttemptId;
    }

    public T run(long taskAttemptId, int attemptNumber) {
        // 注册Task
        SparkEnv.env.blockManager.registerTask(taskAttemptId);
        context =  new TaskContextImpl(stageId,
                                       partitionId,
                                       taskAttemptId,
                                       attemptNumber,
                                       taskMemoryManager,
                                       localProperties);
        TaskContext.setTaskContext(context);
        taskThread = Thread.currentThread();

        if (reasonIfKilled != null) {
            kill(false, reasonIfKilled);
        }

        // TODO: CallerContext实现
        
        try {
            return runTask(context);
        } catch (Throwable e) {
            try {
                context.markTaskFailed(e);
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            context.markTaskCompleted();
            throw new SparkException(e);
        } finally {
            try {
                context.markTaskCompleted();
                // 释放Task内存
                SparkEnv.env.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP);
                SparkEnv.env.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.OFF_HEAP);
                // 唤醒其他因为分配不到内存而阻塞的Task, 重新申请内存
                synchronized (SparkEnv.env.blockManager.memoryManager) {
                    SparkEnv.env.blockManager.memoryManager.notifyAll();
                }
            } finally {
                // 移除TaskContext
                TaskContext.unset();
            }
        }
    }

    public void kill(boolean interruptThread, String reason) {
        assert reason != null;
        reasonIfKilled = reason;
        if (context != null) {
            context.markInterrupted(reason);
        }
        if (interruptThread && taskThread != null) {
            taskThread.interrupt();
        }
    }

    public void setTaskMemoryManager(TaskMemoryManager taskMemoryManager) {
        this.taskMemoryManager = taskMemoryManager;
    }

    public TaskLocation[] preferredLocations() {
        return null;
    }

    public abstract T runTask(TaskContext context) throws Exception;
}
