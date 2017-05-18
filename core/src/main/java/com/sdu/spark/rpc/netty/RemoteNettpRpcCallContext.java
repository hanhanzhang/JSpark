package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.rpc.RpcAddress;

/**
 * @author hanhan.zhang
 * */
public class RemoteNettpRpcCallContext extends NettyRpcCallContext {

    private RpcResponseCallback callback;

    public RemoteNettpRpcCallContext(RpcAddress senderAddress, RpcResponseCallback callback) {
        super(senderAddress);
        this.callback = callback;
    }

    @Override
    public void send(Object message) {

    }
}
