package com.sdu.spark.executor;

/**
 * Metrics tracked during the execution of a task.
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
 *
 * @author hanhan.zhang
 * */
public class TaskMetrics {

    /**
     * Metrics related to shuffle write, defined only in shuffle map stages.
     */
    private ShuffleWriteMetrics shuffleWriteMetrics = new ShuffleWriteMetrics();


    public ShuffleWriteMetrics shuffleWriteMetrics() {
        return shuffleWriteMetrics;
    }
}
