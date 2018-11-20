package com.sdu.spark;

import com.sdu.spark.MapOutputTrackerMessage.*;
import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.RpcEndpoint;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MapOutputTrackerMasterEndpoint}接收到RpcMessage交给MapOutputTrackerMaster处理
 *
 * @author hanhan.zhang
 * */
public class MapOutputTrackerMasterEndpoint extends RpcEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapOutputTrackerMasterEndpoint.class);

    private MapOutputTrackerMaster tracker;
    private SparkConf conf;

    public MapOutputTrackerMasterEndpoint(RpcEnv rpcEnv, MapOutputTrackerMaster tracker, SparkConf conf) {
        super(rpcEnv);
        this.tracker = tracker;
        this.conf = conf;
    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof GetMapOutputStatuses) {
            GetMapOutputStatuses statuses = (GetMapOutputStatuses) msg;
            String hostPort = context.senderAddress().hostPort();
            LOGGER.info("Asked to send map combinerMerge locations for shuffle {} to {}", statuses.shuffleId, hostPort);
            tracker.post(new GetMapOutputMessage(statuses.shuffleId, context));
        } else if (msg instanceof StopMapOutputTracker) {
            LOGGER.info("MapOutputTrackerMasterEndpoint stopped!");
            context.reply(true);
            stop();
        }
    }
}
