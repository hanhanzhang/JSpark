package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.RpcResponseCallback;
import io.netty.channel.Channel;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class TransportClient {

    private Channel channel;


    public void send(ByteBuffer message) {
//        channel.writeAndFlush(message);
    }

    public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
        return 0L;
    }

    public void removeRpcRequest(long requestId) {

    }

}
