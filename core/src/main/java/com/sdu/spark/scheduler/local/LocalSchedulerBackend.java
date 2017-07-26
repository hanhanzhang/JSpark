package com.sdu.spark.scheduler.local;

import com.sdu.spark.SparkApp.StopExecutor;
import com.sdu.spark.SparkContext;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.executor.ExecutorBackend;
import com.sdu.spark.laucher.LauncherBackend;
import com.sdu.spark.launcher.SparkAppHandle.State;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.LiveListenerBus;
import com.sdu.spark.scheduler.SchedulerBackend;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerExecutorAdded;
import com.sdu.spark.scheduler.TaskSchedulerImpl;
import com.sdu.spark.scheduler.TaskState;
import com.sdu.spark.scheduler.cluster.ExecutorInfo;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class LocalSchedulerBackend implements SchedulerBackend, ExecutorBackend {

    private SparkConf conf;
    private TaskSchedulerImpl scheduler;
    private int totalCores;

    private String appId = "local-" + System.currentTimeMillis();
    private RpcEndPointRef localEndpoint = null;
    private LiveListenerBus listenerBus;
    private LauncherBackend launcherBackend;

    public LocalSchedulerBackend(SparkConf conf, TaskSchedulerImpl scheduler, int totalCores) {
        this.conf = conf;
        this.scheduler = scheduler;
        this.totalCores = totalCores;
        this.listenerBus = this.scheduler.sc.listenerBus;
        this.launcherBackend = new LauncherBackend() {
            @Override
            public void onStopRequest() {
                stop(State.KILLED);
            }
        };
        this.launcherBackend.connect();
    }

    @Override
    public void start() {
        RpcEnv rpcEnv = SparkEnv.env.rpcEnv;
        LocalEndpoint executorEndpoint = new LocalEndpoint(rpcEnv, scheduler, this, totalCores);
        localEndpoint = rpcEnv.setRpcEndPointRef("LocalSchedulerBackendEndpoint", executorEndpoint);
        listenerBus.post(new SparkListenerExecutorAdded(
                                        System.currentTimeMillis(),
                                        executorEndpoint.localExecutorId,
                                        new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores)));
        launcherBackend.setAppId(appId);
        launcherBackend.setState(State.RUNNING);
    }

    @Override
    public void statusUpdate(long taskId, TaskState state, ByteBuffer data) {

    }

    private void stop(State state) {
        localEndpoint.ask(new StopExecutor());
        try {
            launcherBackend.setState(state);
        } finally {
            launcherBackend.close();
        }
    }

    private class LocalEndpoint extends RpcEndPoint {

        TaskSchedulerImpl scheduler;
        LocalSchedulerBackend executorBackend;
        String localExecutorId = SparkContext.DRIVER_IDENTIFIER;
        String localExecutorHostname = "localhost";
        private int totalCores;

        public LocalEndpoint(RpcEnv rpcEnv, TaskSchedulerImpl scheduler,
                             LocalSchedulerBackend executorBackend, int totalCores) {
            super(rpcEnv);
            this.scheduler = scheduler;
            this.executorBackend = executorBackend;
            this.totalCores = totalCores;
        }
    }
}
