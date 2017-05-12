package com.sdu.spark.rpc.deploy;

import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;

/**
 * @author hanhan.zhang
 * */
public class Worker extends RpcEndPoint {

    @Override
    public RpcEndPointRef self() {
        return null;
    }

    @Override
    public void receive(Object msg) {

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
}
