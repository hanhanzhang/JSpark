package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.rpc.RpcAddress;

/**
 * @author hanhan.zhang
 * */
public class RemoteNettyRpcCallContext extends NettyRpcCallContext {

    private NettyRpcEnv rpcEnv;
    private RpcResponseCallback callback;

    public RemoteNettyRpcCallContext(RpcAddress senderAddress, NettyRpcEnv rpcEnv, RpcResponseCallback callback) {
        super(senderAddress);
        this.rpcEnv = rpcEnv;
        this.callback = callback;
    }

    @Override
    public void send(Object message) {

    }
}
