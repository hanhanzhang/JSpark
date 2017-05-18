package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcCallContext;

/**
 * @author hanhan.zhang
 * */
public abstract class NettyRpcCallContext extends RpcCallContext {

    public NettyRpcCallContext(RpcAddress senderAddress) {
        super(senderAddress);
    }

    public abstract void send(Object message);

    @Override
    public void reply(Object response) {
        send(response);
    }

    @Override
    public void sendFailure(Throwable cause) {

    }
}
