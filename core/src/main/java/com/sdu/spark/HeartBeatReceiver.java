package com.sdu.spark;

import com.google.common.collect.Maps;
import com.sdu.spark.SparkApp.TaskSchedulerIsSet;
import com.sdu.spark.executor.Heartbeat;
import com.sdu.spark.executor.HeartbeatResponse;
import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.ThreadSafeRpcEndpoint;
import com.sdu.spark.scheduler.TaskScheduler;
import com.sdu.spark.utils.Clock;
import com.sdu.spark.utils.Clock.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static com.sdu.spark.utils.ThreadUtils.newDaemonSingleThreadScheduledExecutor;

/**
 * {@link HeartBeatReceiver}职责:
 *
 * 1: SparkContext实例化过程中创建HeartBeatReceiver, 负责接收来自Executor心跳消息, 接收心跳消息处理流程:
 *
 *   1': {@link #executorLastSeen}若是包含Executor
 *
 *   2': {@link #executorLastSeen}第一次收到Executor心跳
 *
 * @author hanhan.zhang
 * */
public class HeartBeatReceiver extends ThreadSafeRpcEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartBeatReceiver.class);

    public static final String ENDPOINT_NAME = "HeartbeatReceiver";

    private SparkContext sc;
    private Clock clock;

    private TaskScheduler scheduler;

    // key = executorId, value = 上次心跳消息
    private Map<String, Long> executorLastSeen;
    private ScheduledExecutorService eventLoopThread;

    public HeartBeatReceiver(SparkContext sc) {
        this(sc, new SystemClock());
    }

    public HeartBeatReceiver(SparkContext sc, Clock clock) {
        super(sc.env.rpcEnv);
        this.sc = sc;
        this.clock = clock;

        this.executorLastSeen = Maps.newHashMap();
        this.eventLoopThread = newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread");
    }

    @Override
    public void receive(Object msg) {}

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof TaskSchedulerIsSet) {
            scheduler = sc.taskScheduler;
            context.reply(true);
        } else if (msg instanceof Heartbeat) {
            Heartbeat heartbeat = (Heartbeat) msg;
            if (scheduler != null) {
                if (executorLastSeen.containsKey(heartbeat.executorId)) {
                    executorLastSeen.put(heartbeat.executorId, clock.getTimeMillis());
                    eventLoopThread.submit(() -> {
                        boolean unknownExecutor = !scheduler.executorHeartbeatReceived(heartbeat.executorId,
                                                                                       heartbeat.blockManagerId);
                        context.reply(new HeartbeatResponse(unknownExecutor));
                    });
                } else {
                    LOGGER.debug("Received heartbeat from unknown executor {}", heartbeat.executorId);
                    context.reply(new HeartbeatResponse(true));
                }
            } else {
                // Because Executor will sleep several seconds before sending the first "Heartbeat", this
                // case rarely happens. However, if it really happens, log it and ask the executor to
                // register itself again.
                LOGGER.debug("Dropping {} because TaskScheduler is not ready yet", heartbeat);
                context.reply(new HeartbeatResponse(true));
            }
        }
    }
}
