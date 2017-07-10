package com.sdu.spark;

import com.sdu.spark.rpc.RpcAddress;
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
    public void onStart() {

    }

    @Override
    public void onEnd() {

    }

    @Override
    public void onStop() {

    }

    @Override
    public void onConnect(RpcAddress rpcAddress) {

    }

    @Override
    public void onDisconnect(RpcAddress rpcAddress) {

    }

    @Override
    public void receive(Object msg) {

    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {

    }
}
