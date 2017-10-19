package com.sdu.spark;

import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.shuffle.FetchFailedException;
import com.sdu.spark.utils.TaskCompletionListener;
import com.sdu.spark.utils.TaskFailureListener;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public abstract class TaskContext implements Serializable {

    private static ThreadLocal<TaskContext> taskContext = new ThreadLocal<>();

    public static void setTaskContext(TaskContext tc) {
        taskContext.set(tc);
    }

    public static void unset() {
        taskContext.remove();
    }

    public static TaskContext get() {
        return taskContext.get();
    }

    public int getPartitionId() {
        TaskContext tc = taskContext.get();
        if (tc == null) {
            return 0;
        }
        return tc.partitionId();
    }

    public TaskContextImpl empty() {
        return new TaskContextImpl(0, 0, 0, 0, null, new Properties());
    }

    /**
     * Returns true if the task has completed.
     */
    public abstract boolean isCompleted();

    /**
     * Returns true if the task has been killed.
     */
    public abstract boolean isInterrupted();

    /**
     * Returns true if the task is running locally in the driver program.
     * @return false
     */
    public abstract boolean isRunningLocally();

    /**
     * Adds a (Java friendly) listener to be executed on task completion.
     * This will be called in all situations - success, failure, or cancellation. Adding a listener
     * to an already completed task will result in that listener being called immediately.
     *
     * An example use is for HadoopRDD to register a callback to close the input stream.
     *
     * Exceptions thrown by the listener will result in failure of the task.
     */
    public abstract TaskContext addTaskCompletionListener(TaskCompletionListener listener);



    /**
     * Adds a listener to be executed on task failure. Adding a listener to an already failed task
     * will result in that listener being called immediately.
     */
    public abstract TaskContext addTaskFailureListener(TaskFailureListener listener);

    /**
     * The ID of the stage that this task belong to.
     */
    public abstract int stageId();

    /**
     * The ID of the RDD partition that is computed by this task.
     */
    public abstract int partitionId();

    /**
     * How many times this task has been attempted.  The first task attempt will be assigned
     * attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.
     */
    public abstract int attemptNumber();

    /**
     * An ID that is unique to this task attempt (within the same SparkContext, no two task attempts
     * will share the same attempt ID).  This is roughly equivalent to Hadoop's TaskAttemptID.
     */
    public abstract long taskAttemptId();

    /**
     * Get a local property set upstream in the driver, or null if it is missing. See also
     * `org.apache.spark.SparkContext.setLocalProperty`.
     */
    public abstract String getLocalProperty(String key);

    /**
     * If the task is interrupted, throws TaskKilledException with the reason for the interrupt.
     */
    public abstract void killTaskIfInterrupted();

    /**
     * If the task is interrupted, the reason this task was killed, otherwise None.
     */
    public abstract String getKillReason();

    /**
     * Returns the manager for this task's managed memory.
     */
    public abstract TaskMemoryManager taskMemoryManager();


    /**
     * Record that this task has failed due to a fetch failure from a remote host.  This allows
     * fetch-failure handling to get triggered by the driver, regardless of intervening user-code.
     */
    public abstract void setFetchFailed(FetchFailedException fetchFailed);


}
