package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.LocalResponseCallback;
import com.sdu.spark.rpc.RpcAddress;

/**
 * @author hanhan.zhang
 * */
public class LocalNettyRpcCallContext extends NettyRpcCallContext {

    private LocalResponseCallback<Object> callback;

    public LocalNettyRpcCallContext(RpcAddress senderAddress, LocalResponseCallback<Object> callback) {
        super(senderAddress);
        this.callback = callback;
    }

    @Override
    public void send(Object message) {
        callback.onSuccess(message);
    }
}
