package com.sdu.spark.scheduler.cluster;

import com.sdu.spark.deploy.client.StandaloneAppClient;
import com.sdu.spark.deploy.client.StandaloneAppClientListener;
import com.sdu.spark.scheduler.TaskSchedulerImpl;

import java.util.List;
import java.util.concurrent.Future;

/**
 * @author hanhan.zhang
 * */
public class StandaloneSchedulerBackend extends CoarseGrainedSchedulerBackend implements StandaloneAppClientListener {

    // Spark Master交互客户端
    private StandaloneAppClient client;

    public StandaloneSchedulerBackend(TaskSchedulerImpl scheduler) {
        super(scheduler);
    }

    @Override
    public Future<Boolean> doRequestTotalExecutors(int requestedTotal) {
        return null;
    }

    @Override
    public Future<Boolean> doKillExecutors(List<String> executorIds) {
        return null;
    }

    @Override
    public void connected(String appId) {

    }

    @Override
    public void disconnected() {

    }

    @Override
    public void dead(String reason) {

    }

    @Override
    public void executorAdded(String fullId, String workerId, String hostPort, int cores, int memory) {

    }

    @Override
    public void executorRemove(String fullId, String message, int exitStatus, boolean workerLost) {

    }

    @Override
    public void workerRemoved(String workerId, String host, String message) {

    }
}
