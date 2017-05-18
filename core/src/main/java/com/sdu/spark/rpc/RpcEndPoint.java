package com.sdu.spark.rpc;

/**
 * Rpc节点
 *
 * @author hanhan.zhang
 * */
public abstract class RpcEndPoint {

    // Rpc节点的引用节点
    public abstract RpcEndPointRef self();

    public abstract void onStart();

    public abstract void onEnd();

    public abstract void onStop();

    public abstract void onConnect(RpcAddress rpcAddress);

    public abstract void onDisconnect(RpcAddress rpcAddress);

    /**
     * Rpc消息处理但不响应
     * */
    public abstract void receive(Object msg);

    /**
     * Rpc消息处理需做响应
     * */
    public abstract void receiveAndReply(Object msg, RpcCallContext context);
}
