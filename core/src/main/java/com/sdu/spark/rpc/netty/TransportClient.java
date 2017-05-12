package com.sdu.spark.rpc.netty;

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

}
