package com.sdu.spark.scheduler;

/**
 * @author hanhan.zhang
 * */
public interface TaskScheduler {

    void start();

    void executorLost(String executorId, String reason);

    void workerRemoved(String workerId, String host, String message);
}
