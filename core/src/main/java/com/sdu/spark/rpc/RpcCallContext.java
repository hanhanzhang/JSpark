package com.sdu.spark.rpc;

/**
 * @author hanhan.zhang
 * */
public abstract class RpcCallContext {

    /**
     * 消息发送方地址
     * */
    protected RpcAddress senderAddress;

    public RpcCallContext(RpcAddress senderAddress) {
        this.senderAddress = senderAddress;
    }

    public abstract void reply(Object response);

    public abstract void sendFailure(Throwable cause);

    public RpcAddress senderAddress() {
        return senderAddress;
    }
}
