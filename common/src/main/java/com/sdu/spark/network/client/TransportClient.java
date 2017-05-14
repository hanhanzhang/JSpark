package com.sdu.spark.network.client;

import io.netty.channel.Channel;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class TransportClient {

    private Channel channel;

    private TransportResponseHandler responseHandler;

    public TransportClient(Channel channel, TransportResponseHandler responseHandler) {
        this.channel = channel;
        this.responseHandler = responseHandler;
    }

    public void send(ByteBuffer message) {
//        channel.writeAndFlush(message);
    }

    public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
        return 0L;
    }

    public void removeRpcRequest(long requestId) {

    }

}
