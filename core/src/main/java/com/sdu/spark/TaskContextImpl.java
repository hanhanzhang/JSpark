package com.sdu.spark;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.shuffle.FetchFailedException;
import com.sdu.spark.utils.TaskCompletionListener;
import com.sdu.spark.utils.TaskFailureListener;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class TaskContextImpl extends TaskContext {
    public int stageId;
    public int partitionId;
    public long taskAttemptId;
    public int attemptNumber;
    public TaskMemoryManager taskMemoryManager;
    public Properties localProperties;

    /**
     * Task运行回调函数(无需序列化)
     * */
    private transient List<TaskCompletionListener> onCompleteCallbacks = Lists.newArrayList();
    private transient List<TaskFailureListener> onFailureCallbacks = Lists.newArrayList();



    /**
     * Task运行状态
     * */
    private String reasonIfKilled;
    private boolean completed = false;
    private boolean failed = false;
    private Throwable failure = null;
    private volatile FetchFailedException fetchFailedException;

    public TaskContextImpl(int stageId, int partitionId, long taskAttemptId, int attemptNumber,
                           TaskMemoryManager taskMemoryManager, Properties localProperties) {
        this.stageId = stageId;
        this.partitionId = partitionId;
        this.taskAttemptId = taskAttemptId;
        this.attemptNumber = attemptNumber;
        this.taskMemoryManager = taskMemoryManager;
        this.localProperties = localProperties;
    }

    @Override
    public boolean isCompleted() {
        synchronized (this) {
            return completed;
        }
    }

    @Override
    public boolean isInterrupted() {
        synchronized (this) {
            return !Strings.isNullOrEmpty(reasonIfKilled);
        }
    }

    @Override
    public boolean isRunningLocally() {
        return false;
    }

    @Override
    public TaskContext addTaskCompletionListener(TaskCompletionListener listener) {
        synchronized (this) {
            if (completed) {
                listener.onTaskCompletion(this);
            } else {
                onCompleteCallbacks.add(listener);
            }
            return this;
        }
    }

    @Override
    public TaskContext addTaskFailureListener(TaskFailureListener listener) {
        synchronized (this) {
            if (failed) {
                listener.onTaskFailure(this, failure);
            } else {
                onFailureCallbacks.add(listener);
            }
            return this;
        }
    }

    @Override
    public int stageId() {
        return stageId;
    }

    @Override
    public int partitionId() {
        return partitionId;
    }

    @Override
    public int attemptNumber() {
        return attemptNumber;
    }

    @Override
    public long taskAttemptId() {
        return taskAttemptId;
    }

    @Override
    public String getLocalProperty(String key) {
        return localProperties.getProperty(key);
    }

    @Override
    public void killTaskIfInterrupted() {
        String reason = reasonIfKilled;
        if (StringUtils.isNotEmpty(reason)) {
            throw new TaskKilledException(reason);
        }
    }

    @Override
    public String getKillReason() {
        return reasonIfKilled;
    }

    @Override
    public TaskMemoryManager taskMemoryManager() {
        return taskMemoryManager;
    }

    @Override
    public void setFetchFailed(FetchFailedException fetchFailed) {
        fetchFailedException = fetchFailed;
    }

    public void markInterrupted(String reason) {
        reasonIfKilled = reason;
    }

    public void markTaskCompleted() {
        synchronized (this) {
            if (completed) {
                return;
            }
            completed = true;
            invokeTaskCompleteListener();
        }
    }

    public void markTaskFailed(Throwable error) {
        synchronized (this) {
            if (failed) {
                return;
            }
            failed = true;
            failure = error;
            invokeTaskFailureListener();
        }
    }

    private void invokeTaskFailureListener() {
        if (onFailureCallbacks != null) {
            onFailureCallbacks.forEach(listener -> listener.onTaskFailure(this, failure));
        }
    }

    private void invokeTaskCompleteListener() {
        if (onCompleteCallbacks != null) {
            onCompleteCallbacks.forEach(listener -> listener.onTaskCompletion(this));
        }
    }
}
