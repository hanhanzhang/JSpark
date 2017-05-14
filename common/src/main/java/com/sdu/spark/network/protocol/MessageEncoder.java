package com.sdu.spark.network.protocol;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 消息编码[可被多个{@link ChannelHandler}共享], 单例模式
 *
 * @author hanhan.zhang
 * */
@ChannelHandler.Sharable
public class MessageEncoder extends MessageToMessageEncoder<Message> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageEncoder.class);

    public static final MessageEncoder INSTANCE = new MessageEncoder();

    private MessageEncoder() {}

    @Override
    protected void encode(ChannelHandlerContext ctx, Message in, List<Object> out) throws Exception {

    }
}
