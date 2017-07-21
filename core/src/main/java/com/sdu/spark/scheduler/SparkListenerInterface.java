package com.sdu.spark.scheduler;

import com.sdu.spark.scheduler.SparkListenerEvent.*;

/**
 * @author hanhan.zhang
 * */
public interface SparkListenerInterface {

    /**
     * Called when a stage completes successfully or fails, with information on the completed stage.
     */
    void onStageCompleted(SparkListenerStageCompleted stageCompleted);

    /**
     * Called when a stage is submitted
     */
    void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted);

    /**
     * Called when a task starts
     */
    void onTaskStart(SparkListenerTaskStart taskStart);

    /**
     * Called when a task begins remotely fetching its result (will not be called for tasks that do
     * not need to fetch the result remotely).
     */
    void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult);

    /**
     * Called when a task ends
     */
    void onTaskEnd(SparkListenerTaskEnd taskEnd);

    /**
     * Called when a job starts
     */
    void onJobStart(SparkListenerJobStart jobStart);

    /**
     * Called when a job ends
     */
    void onJobEnd(SparkListenerJobEnd jobEnd);

    /**
     * Called when environment properties have been updated
     */
    void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate);

    /**
     * Called when a new block manager has joined
     */
    void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded);

    /**
     * Called when an existing block manager has been removed
     */
    void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved);

    /**
     * Called when an RDD is manually unpersisted by the application
     */
    void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD);

    /**
     * Called when the application starts
     */
    void onApplicationStart(SparkListenerApplicationStart applicationStart);

    /**
     * Called when the application ends
     */
    void onApplicationEnd(SparkListenerApplicationEnd applicationEnd);

    /**
     * Called when the driver registers a new executor.
     */
    void onExecutorAdded(SparkListenerExecutorAdded executorAdded);

    /**
     * Called when the driver removes an executor.
     */
    void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved);

    /**
     * Called when the driver blacklists an executor for a Spark application.
     */
    void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted);

    /**
     * Called when the driver re-enables a previously blacklisted executor.
     */
    void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted);

    /**
     * Called when the driver blacklists a node for a Spark application.
     */
    void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted);

    /**
     * Called when the driver re-enables a previously blacklisted node.
     */
    void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted);

    /**
     * Called when the driver receives a block update info.
     */
    void onBlockUpdated(SparkListenerBlockUpdated blockUpdated);

    /**
     * Called when other events like SQL-specific events are posted.
     */
    void onOtherEvent(SparkListenerEvent event);

}
