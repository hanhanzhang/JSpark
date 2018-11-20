package com.sdu.spark.scheduler;

import com.sdu.spark.SparkEnv;
import com.sdu.spark.SparkException;
import com.sdu.spark.TaskContext;
import com.sdu.spark.TaskContextImpl;
import com.sdu.spark.memory.MemoryMode;
import com.sdu.spark.memory.TaskMemoryManager;

import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public abstract class Task<T> {

    // TODO: Metric, TaskMetrics
//    private byte[] serializedTaskMetrics;
//    private TaskMetrics metrics;

    protected int stageId;
    private int stageAttemptId;
    protected int partitionId;
    private transient Properties localProperties;
    private int jobId;
    private String appId;
    private String appAttemptId;

    private TaskMemoryManager taskMemoryManager;

    // TaskSetManager设置
    protected long epoch;
    private transient TaskContextImpl context;
    private transient volatile String reasonIfKilled = null;
    // 运行Task的线程
    private transient Thread taskThread;

    protected long executorDeserializeTime = 0L;
    protected long executorDeserializeCpuTime = 0L;

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

    public void setTaskMemoryManager(TaskMemoryManager taskMemoryManager) {
        this.taskMemoryManager = taskMemoryManager;
    }

    public void setLocalProperties(Properties properties) {
        this.localProperties = properties;
    }

    public long epoch() {
        return epoch;
    }

    public int stageAttemptId() {
        return stageAttemptId;
    }

    public TaskContext context() {
        return context;
    }

    public String reasonIfKilled() {
        return reasonIfKilled;
    }

    public long executorDeserializeTime() {
        return executorDeserializeTime;
    }

    public long executorDeserializeCpuTime() {
        return executorDeserializeCpuTime;
    }

    /**
     * Called by {@link com.sdu.spark.executor.Executor} to run this task
     * */
    public T run(long taskAttemptId, int attemptNumber) {
        // 注册Task
        SparkEnv.env.blockManager.registerTask(taskAttemptId);
        context =  new TaskContextImpl(stageId, partitionId, taskAttemptId, attemptNumber, taskMemoryManager, localProperties);
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

    public abstract T runTask(TaskContext context) throws Exception;

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

    // TODO: TaskMetric
//    public List<AccumulatorV2<?, ?>> collectAccumulatorUpdates(boolean taskFailed) {
//
//    }

    public TaskLocation[] preferredLocations() {
        return new TaskLocation[]{};
    }
}
