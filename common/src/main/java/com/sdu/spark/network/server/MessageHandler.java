package com.sdu.spark.network.server;

import com.sdu.spark.network.protocol.Message;

/**
 * Rpc Message处理
 *
 * @author hanhan.zhang
 * */
public abstract class MessageHandler<T extends Message> {

    public abstract void handle(T message) throws Exception;

    public abstract void channelActive();

    public abstract void exceptionCaught(Throwable cause);

    public abstract void channelInActive();
}
