package com.sdu.spark.rpc;

/**
 * Rpc节点
 *
 * @author hanhan.zhang
 * */
public abstract class RpcEndpoint {

    public RpcEnv rpcEnv;

    public RpcEndpoint(RpcEnv rpcEnv) {
        this.rpcEnv = rpcEnv;
    }

    // Rpc节点的引用节点
    public RpcEndpointRef self() {
        assert rpcEnv != null : "nettyEnv has not been initialized";
        return rpcEnv.endPointRef(this);
    }

    // Rpc消息处理但不响应
    public void receive(Object msg) {}

    // Rpc消息处理需做响应
    public void receiveAndReply(Object msg, RpcCallContext context) {}

    /**
     * Invoked when any exception is thrown during handling messages.
     */
    public void onError(Throwable cause) {}

    /**
     * Invoked when `remoteAddress` is connected to the current node.
     */
    public void onConnected(RpcAddress remoteAddress) {}

    /**
     * Invoked when `remoteAddress` is lost.
     */
    public void onDisconnected(RpcAddress remoteAddress) {}

    /**
     * Invoked when some network error happens in the connection between the current node and
     * `remoteAddress`.
     */
    public void onNetworkError(Throwable e, RpcAddress remoteAddress) {}

    /**
     * Invoked before [[RpcEndpoint]] starts to handle any message.
     */
    public void onStart() {}

    /**
     * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
     * use it to send or ask messages.
     */
    public void onStop() {}

    public final void stop() {
        if (self() != null) {
            rpcEnv.stop(self());
        }
    }
}
