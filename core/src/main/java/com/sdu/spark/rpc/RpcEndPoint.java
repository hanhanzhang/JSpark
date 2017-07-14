package com.sdu.spark.rpc;

/**
 * Rpc节点
 *
 * @author hanhan.zhang
 * */
public abstract class RpcEndPoint {

    // Rpc节点的引用节点
    public abstract RpcEndPointRef self();

    public void onStart() {}

    public void onEnd() {}

    public void onStop() {}

    public void onConnect(RpcAddress rpcAddress) {}

    public void onDisconnect(RpcAddress rpcAddress) {}

    // Rpc消息处理但不响应
    public void receive(Object msg) {}

    // Rpc消息处理需做响应
    public void receiveAndReply(Object msg, RpcCallContext context) {}
}
