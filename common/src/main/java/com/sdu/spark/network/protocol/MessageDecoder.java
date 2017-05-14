package com.sdu.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 消息解码[可被多个{@link ChannelHandler}共享], 单例模式
 *
 * @author hanhan.zhang
 * */
@ChannelHandler.Sharable
public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageDecoder.class);

    public static final MessageDecoder INSTANCE = new MessageDecoder();

    private MessageDecoder() {}

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Message.Type msgType = Message.Type.decode(in);
        Message msg = decode(msgType, in);
        assert msg.type() == msgType;
        LOGGER.trace("Received message {} : {}", msgType, msg);
        out.add(msg);
    }

    private Message decode(Message.Type msgType, ByteBuf in) {
        switch (msgType) {
            case RpcRequest:
                return RpcRequest.decode(in);
            case RpcResponse:
                return RpcResponse.decode(in);
            case RpcFailure:
                return RpcFailure.decode(in);
            case OneWayMessage:
                return OneWayMessage.decode(in);
            default:
                throw new IllegalArgumentException("Unexpected message type: " + msgType);
        }
    }
}
