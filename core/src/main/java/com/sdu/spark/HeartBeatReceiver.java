package com.sdu.spark;

import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;

/**
 * @author hanhan.zhang
 * */
public class HeartBeatReceiver extends RpcEndPoint {

    public static final String ENDPOINT_NAME = "HeartbeatReceiver";

    @Override
    public RpcEndPointRef self() {
        return null;
    }

    @Override
    public void receive(Object msg) {

    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {

    }
}
