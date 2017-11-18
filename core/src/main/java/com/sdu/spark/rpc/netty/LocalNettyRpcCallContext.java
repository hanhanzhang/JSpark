package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.RpcAddress;

import java.util.concurrent.CompletableFuture;

/**
 * @author hanhan.zhang
 * */
public class LocalNettyRpcCallContext extends NettyRpcCallContext {

//    private LocalResponseCallback<Object> callback;
    CompletableFuture<Object> p;

    public LocalNettyRpcCallContext(RpcAddress senderAddress, CompletableFuture<Object> p) {
        super(senderAddress);
        this.p = p;
    }

    @Override
    public void send(Object message) {
        p.complete(message);
    }
}
