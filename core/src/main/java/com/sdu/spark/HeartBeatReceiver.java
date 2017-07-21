package com.sdu.spark;

import com.sdu.spark.executor.CoarseGrainedExecutorBackend;
import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.scheduler.TaskScheduler;
import com.sdu.spark.utils.Clock;
import com.sdu.spark.utils.Clock.*;
import com.sdu.spark.SparkApp.*;

/**
 * Driver接收{@link CoarseGrainedExecutorBackend}发送的心跳
 *
 * @author hanhan.zhang
 * */
public class HeartBeatReceiver extends RpcEndPoint {

    public static final String ENDPOINT_NAME = "HeartbeatReceiver";

    private SparkContext sc;
    private Clock clock;

    private TaskScheduler taskScheduler;

    public HeartBeatReceiver(SparkContext sc) {
        this(sc, new SystemClock());
    }

    public HeartBeatReceiver(SparkContext sc, Clock clock) {
        this.sc = sc;
        this.clock = clock;
    }

    @Override
    public RpcEndPointRef self() {
        return sc.env.rpcEnv.endPointRef(this);
    }

    @Override
    public void receive(Object msg) {}

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof TaskSchedulerIsSet) {
            taskScheduler = sc.taskScheduler;
            context.reply(true);
        }
    }
}
