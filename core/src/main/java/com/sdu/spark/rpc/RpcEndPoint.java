package com.sdu.spark.rpc;

/**
 * Rpc节点
 *
 * @author hanhan.zhang
 * */
public abstract class RpcEndPoint {

    public RpcEnv rpcEnv;

    public RpcEndPoint(RpcEnv rpcEnv) {
        this.rpcEnv = rpcEnv;
    }

    // Rpc节点的引用节点
    public RpcEndPointRef self() {
        assert rpcEnv != null : "nettyEnv has not been initialized";
        return rpcEnv.endPointRef(this);
    }

    public void onStart() {}

    public void onEnd() {}

    public void onStop() {}

    public void onConnect(RpcAddress rpcAddress) {}

    public void onDisconnect(RpcAddress rpcAddress) {}

    // Rpc消息处理但不响应
    public void receive(Object msg) {}

    // Rpc消息处理需做响应
    public void receiveAndReply(Object msg, RpcCallContext context) {}

    public final void stop() {
        if (self() != null) {
            rpcEnv.stop(self());
        }
    }
}
