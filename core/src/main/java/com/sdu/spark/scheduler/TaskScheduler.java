package com.sdu.spark.scheduler;

import com.sdu.spark.executor.ExecutorExitCode.ExecutorLossReason;
import com.sdu.spark.storage.BlockManagerId;

/**
 * TaskSchedule负责从DAGSchedule接收TaskSet并创建TaskSetManager对TaskSet管理.
 *
 * TaskSchedule由SchedulerBackend将Task提交Executor上运行.
 *
 * @author hanhan.zhang
 * */
public interface TaskScheduler {

    String appId = "spark-application-" + System.currentTimeMillis();

    Pool rootPool();

    SchedulingMode schedulingMode();

    void start();

    // Invoked after system has successfully initialized (typically in spark context).
    // Yarn uses this to bootstrap allocation of resources based on preferred locations,
    // wait for slave registrations, etc.
    void postStartHook();

    // Disconnect from the cluster.
    void stop();

    // Submit a sequence of tasks to run.
    void submitTasks(TaskSet taskSet);

    // Cancel a stage.
    void cancelTasks(int stageId, boolean interruptThread);

    /**
     * Kills a task attempt.
     *
     * @return Whether the task was successfully killed.
     */
    boolean killTaskAttempt(int taskId, boolean interruptThread, String reason);

    // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
    void setDAGScheduler(DAGScheduler dagScheduler);

    // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
    void defaultParallelism();

    /**
     * Update metrics for in-progress tasks and let the master know that the BlockManager is still
     * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
     * indicating that the block manager should re-register.
     */
    boolean executorHeartbeatReceived(String execId, BlockManagerId blockManagerId);

    /**
     * Get an application ID associated with the job.
     *
     * @return An application ID
     */
    default String applicationId() {
        return appId;
    }

    /**
     * Process a lost executor
     */
    void executorLost(String executorId, ExecutorLossReason reason);

    /**
     * Process a removed worker
     */
    void workerRemoved(String workerId, String host, String message);

    /**
     * Get an application's attempt ID associated with the job.
     *
     * @return An application's Attempt ID
     */
    String applicationAttemptId();
}
