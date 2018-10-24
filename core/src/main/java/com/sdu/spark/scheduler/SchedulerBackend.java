package com.sdu.spark.scheduler;

import java.util.Map;

import static java.lang.String.format;

/**
 * SchedulerBackend是TaskSchedule的调度后端接口. TaskSchedule给Task分配资源是通过ScheduleBackend实现, ScheduleBackend给
 * Task分配资源后将与分配给Task的Executor通信并启动Task.
 *
 *
 * @author hanhan.zhang
 * */
public abstract class SchedulerBackend {

    private String appId = format("spark-application-%s", System.currentTimeMillis());

    public abstract void start();

    public abstract void stop();

    public abstract void reviveOffers();

    public Map<String, String> getDriverLogUrls() {
        return null;
    }

    public String applicationAttemptId() {
        return null;
    }

    public String applicationId() {
        return appId;
    }

    public boolean isReady() {
        return true;
    }

    public void killTask(long taskId, String executorId, boolean interruptThread, String reason) {
        throw new UnsupportedOperationException();
    }
}
