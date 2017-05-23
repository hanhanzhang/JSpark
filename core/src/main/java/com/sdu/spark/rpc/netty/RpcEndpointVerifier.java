package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.*;

/**
 * RpcEndPoint用于询问'NAME'的Rpc节点是否存在
 *
 * @author hanhan.zhang
 * */
public class RpcEndpointVerifier extends RpcEndPoint {

    private RpcEnv rpcEnv;

    private Dispatcher dispatcher;

    public RpcEndpointVerifier(RpcEnv rpcEnv, Dispatcher dispatcher) {
        this.rpcEnv = rpcEnv;
        this.dispatcher = dispatcher;
    }

    @Override
    public RpcEndPointRef self() {
        return rpcEnv.endPointRef(this);
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
