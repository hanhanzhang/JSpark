package com.sdu.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
        Object body = null;
        long bodyLength = 0L;
        boolean isBodyInFrame = false;
        if (in.body() != null) {
            try {
                bodyLength = in.body().size();
                isBodyInFrame = in.isBodyInFrame();
                body = in.body().convertToNetty();
            } catch (IOException e) {
                in.body().release();
                if (in instanceof AbstractResponseMessage) {
                    AbstractResponseMessage resp = (AbstractResponseMessage) in;
                    String error = e.getMessage() != null ? e.getMessage() : "null";
                    LOGGER.error("Error processing {} for client {}", in, ctx.channel().remoteAddress(), e);
                    encode(ctx, resp.createFailureResponse(error), out);
                } else {
                    throw e;
                }
                return;
            }

        }
        /**
         * 消息编码:
         *
         * +-------------+------+---------------+----------------+
         * |    header   | Type | MessageHeader |   MessageBody  |
         * +-------------+------+---------------+----------------+
         * | FrameLength |               FrameBody               |
         * +-------------+--------------------------------------=+
         *
         * ByteBuf.length = FrameLength + TypeBodyLength + MessageLength
         * */
        Message.Type msgType = in.type();
        int headerLength = 8 + msgType.encodedLength() + in.encodedLength();
        long frameLength = headerLength + (isBodyInFrame ? bodyLength : 0);
        ByteBuf header = ctx.alloc().heapBuffer(headerLength);

        // 写入传输内容
        header.writeLong(frameLength);
        msgType.encode(header);
        in.encode(header);

        assert header.writableBytes() == 0;

        if(body != null) {
            out.add(new MessageWithHeader(in.body(), header, body, bodyLength));
        } else {
            out.add(body);
        }
    }
}
