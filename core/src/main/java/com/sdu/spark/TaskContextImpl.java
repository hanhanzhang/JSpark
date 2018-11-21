package com.sdu.spark;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.sdu.spark.executor.TaskMetrics;
import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.shuffle.FetchFailedException;
import com.sdu.spark.utils.TaskCompletionListener;
import com.sdu.spark.utils.TaskCompletionListenerException;
import com.sdu.spark.utils.TaskFailureListener;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 *
 * TODO: Task Metric / Metric System
 * @author hanhan.zhang
 * */
public class TaskContextImpl extends TaskContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskContextImpl.class);

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



    /**Task运行状态*/
    private volatile String reasonIfKilled;
    private boolean completed = false;
    private boolean failed = false;
    private Throwable failure = null;
    private volatile FetchFailedException fetchFailedException;

    public TaskContextImpl(int stageId,
                           int partitionId,
                           long taskAttemptId,
                           int attemptNumber,
                           TaskMemoryManager taskMemoryManager,
                           Properties localProperties) {
        this.stageId = stageId;
        this.partitionId = partitionId;
        this.taskAttemptId = taskAttemptId;
        this.attemptNumber = attemptNumber;
        this.taskMemoryManager = taskMemoryManager;
        this.localProperties = localProperties;
    }

    @Override
    public synchronized boolean isCompleted() {
        return completed;
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
    public synchronized TaskContext addTaskCompletionListener(TaskCompletionListener listener) {
        if (completed) {
            listener.onTaskCompletion(this);
        } else {
            onCompleteCallbacks.add(listener);
        }
        return this;
    }

    @Override
    public synchronized TaskContext addTaskFailureListener(TaskFailureListener listener) {
        if (failed) {
            listener.onTaskFailure(this, failure);
        } else {
            onFailureCallbacks.add(listener);
        }
        return this;
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
    public TaskMetrics taskMetrics() {
        // TODO: 待实现
        return new TaskMetrics();
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

    @Override
    public FetchFailedException fetchFailed() {
        return fetchFailedException;
    }

    public void markInterrupted(String reason) {
        reasonIfKilled = reason;
    }

    public synchronized void markTaskCompleted() {
        if (completed) {
            return;
        }
        completed = true;
        invokeListeners(onCompleteCallbacks,
                        "TaskCompletionListener",
                        null,
                        listener -> listener.onTaskCompletion(this));
    }

    public synchronized void markTaskFailed(Throwable error) {
        if (failed) {
            return;
        }
        failed = true;
        failure = error;
        invokeListeners(onFailureCallbacks,
                        "TaskFailureListener",
                        error,
                        listener -> listener.onTaskFailure(this, error));
    }


    private <T> void invokeListeners(List<T> listeners,
                                     String name,
                                     Throwable error,
                                     TaskStatusCallback<T> callback) {
        List<String> errorMsg = Lists.newArrayList();
        // 按照注册顺序调用
        Lists.reverse(listeners).forEach(listener -> {
            try {
                callback.call(listener);
            } catch (Throwable e) {
                errorMsg.add(e.getMessage());
                LOGGER.error("Exception occurred when invoke {} listener", name, e);
            }
        });
        if (errorMsg.size() > 0) {
            throw new TaskCompletionListenerException(errorMsg);
        }
    }

    private interface TaskStatusCallback<T> {

        void call(T listener);

    }
}
