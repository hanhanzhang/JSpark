package com.sdu.spark.scheduler;

/**
 * @author hanhan.zhang
 * */
public interface SchedulerBackend {

    void start();

    void stop();

    void reviveOffers();

    void killTask(long taskId, String executorId, boolean interruptThread, String reason);
}
