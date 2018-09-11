package com.sdu.spark.scheduler;

import com.sdu.spark.executor.ExecutorExitCode.*;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.scheduler.action.JobAction;
import com.sdu.spark.utils.CallSite;

import java.util.List;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public interface DAGSchedulerEvent {

    class JobSubmitted<T, U> implements DAGSchedulerEvent {
        public int jobId;
        public RDD<T> finalRDD;
        public JobAction<T, U> jobAction;
        public List<Integer> partitions;
        public JobListener listener;
        public Properties properties;

        public JobSubmitted(int jobId,
                            RDD<T> finalRDD,
                            JobAction<T, U> jobAction,
                            List<Integer> partitions,
                            JobListener listener,
                            Properties properties) {
            this.jobId = jobId;
            this.finalRDD = finalRDD;
            this.jobAction = jobAction;
            this.partitions = partitions;
            this.listener = listener;
            this.properties = properties;
        }
    }

    class JobCancelled implements DAGSchedulerEvent {
        public int jobId;
        public String reason;

        public JobCancelled(int jobId, String reason) {
            this.jobId = jobId;
            this.reason = reason;
        }
    }

    class ExecutorLost implements DAGSchedulerEvent {
        public String execId;
        public ExecutorLossReason reason;

        public ExecutorLost(String execId, ExecutorLossReason reason) {
            this.execId = execId;
            this.reason = reason;
        }
    }

    class TaskSetFailed implements DAGSchedulerEvent {
        public TaskSet taskSet;
        public String reason;
        public Throwable exception;

        public TaskSetFailed(TaskSet taskSet, String reason, Throwable exception) {
            this.taskSet = taskSet;
            this.reason = reason;
            this.exception = exception;
        }
    }

    class GettingResultEvent implements DAGSchedulerEvent {
        public TaskInfo taskInfo;

        public GettingResultEvent(TaskInfo taskInfo) {
            this.taskInfo = taskInfo;
        }
    }

    class CompletionEvent implements DAGSchedulerEvent {
        private final Task<?> task;
        private final TaskEndReason reason;
        private final Object result;
        private final TaskInfo taskInfo;

        public CompletionEvent(Task<?> task, TaskEndReason reason, Object result, TaskInfo taskInfo) {
            this.task = task;
            this.reason = reason;
            this.result = result;
            this.taskInfo = taskInfo;
        }

        public Task<?> getTask() {
            return task;
        }

        public TaskEndReason getReason() {
            return reason;
        }

        public Object getResult() {
            return result;
        }

        public TaskInfo getTaskInfo() {
            return taskInfo;
        }
    }
}
