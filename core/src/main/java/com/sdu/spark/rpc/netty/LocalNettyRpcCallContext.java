package com.sdu.spark.rpc.netty;

import com.google.common.util.concurrent.SettableFuture;
import com.sdu.spark.rpc.RpcAddress;

/**
 * @author hanhan.zhang
 * */
public class LocalNettyRpcCallContext extends NettyRpcCallContext {

//    private LocalResponseCallback<Object> callback;
    SettableFuture<Object> p;

    public LocalNettyRpcCallContext(RpcAddress senderAddress, SettableFuture<Object> p) {
        super(senderAddress);
        this.p = p;
    }

    @Override
    public void send(Object message) {
        p.set(message);
    }
}
