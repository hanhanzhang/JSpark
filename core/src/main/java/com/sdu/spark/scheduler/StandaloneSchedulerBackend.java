package com.sdu.spark.scheduler;

import com.sdu.spark.deploy.client.StandaloneAppClientListener;
import com.sdu.spark.scheduler.cluster.CoarseGrainedSchedulerBackend;

/**
 * @author hanhan.zhang
 * */
public class StandaloneSchedulerBackend extends CoarseGrainedSchedulerBackend implements StandaloneAppClientListener {

    public StandaloneSchedulerBackend(TaskSchedulerImpl scheduler) {
        super(scheduler);
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
