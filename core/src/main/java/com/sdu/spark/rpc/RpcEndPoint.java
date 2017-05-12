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

    public abstract void receive(Object msg);
}
