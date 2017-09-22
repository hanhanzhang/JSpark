package com.sdu.spark.network.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.LinkedList;

/**
 * Netty消息解码:
 *
 * 1: 首先读取8字节的FrameLength, FrameBody = FrameLength - 8
 *
 * 2: FrameBody = messageType + messageMeta + messageBody
 *
 * 3: 读取ByteBuf交给{@link com.sdu.spark.network.protocol.MessageDecoder}处理
 *
 * Note:
 *
 *  线程不安全, Netty处理链对每个SocketChannel需重建(也就是说，每个SocketChannel有自己的TransportFrameDecoder, 这是由于
 *
 *  TransportFrameDecoder是有状态的)
 *
 * @author hanhan.zhang
 * */
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = "frameDecoder";

    /**
     * FrameLength, 8字节
     * */
    private static final int LENGTH_SIZE = 8;
    /**
     * FrameLength最大值
     * */
    private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
    private static final int UNKNOWN_FRAME_SIZE = -1;

    private final LinkedList<ByteBuf> buffers = new LinkedList<>();
    /**
     * 读取FrameLength
     * */
    private final ByteBuf frameLenBuf = Unpooled.buffer(LENGTH_SIZE, LENGTH_SIZE);

    /**
     * 记录FrameBody总大小
     * */
    private long totalSize = 0;
    /**
     * 记录下一个FrameBody的大小
     * */
    private long nextFrameSize = UNKNOWN_FRAME_SIZE;
    private volatile Interceptor interceptor;


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        buffers.add(buf);

        totalSize += buf.readableBytes();

        // 循环读取数据
        while (!buffers.isEmpty()) {
            if (interceptor != null) {

            } else {
                ByteBuf frame = decodeNext();
                if (frame == null) {
                    break;
                }
                ctx.fireChannelRead(frame);
            }
        }
    }

    /**
     * 读取FrameBody
     * */
    private ByteBuf decodeNext() throws Exception {
        long frameSize = decodeFrameSize();
        if (frameSize == UNKNOWN_FRAME_SIZE || totalSize < frameSize) {
            // FrameBody已读取结束
            return null;
        }

        // 重置
        nextFrameSize = UNKNOWN_FRAME_SIZE;

        int remaining = (int) frameSize;
        if (buffers.getFirst().readableBytes() >= remaining) {
            return nextBufferForFrame(remaining);
        }

        // Otherwise, create a composite buffer.
        CompositeByteBuf frame = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
        while (remaining > 0) {
            ByteBuf next = nextBufferForFrame(remaining);
            remaining -= next.readableBytes();
            frame.addComponent(next).writerIndex(frame.writerIndex() + next.readableBytes());
        }
        assert remaining == 0;
        return frame;
    }

    private ByteBuf nextBufferForFrame(int byteToRead) {
        ByteBuf buf = buffers.getFirst();
        if (buf.readableBytes() > byteToRead) {
            ByteBuf frame = buf.readSlice(byteToRead);
            totalSize -= byteToRead;
            return frame;
        }

        ByteBuf frame = buf;
        totalSize -= frame.readableBytes();
        buffers.removeFirst();
        return frame;
    }



    private long decodeFrameSize() {
        // 发送的数据包: FrameLength(8字节) + FrameBody, 故totalSize至少为8字节
        if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < LENGTH_SIZE) {
            return nextFrameSize;
        }

        ByteBuf first = buffers.getFirst();
        if (first.readableBytes() >= LENGTH_SIZE) {
            // 读取FrameBody
            nextFrameSize = first.readLong() - LENGTH_SIZE;
            totalSize -= LENGTH_SIZE;
            if (!first.isReadable()) {
                // 若ByteBuf没有可读取的数据, 则删除ByteBuf并释放
                buffers.removeFirst().release();
            }
            return nextFrameSize;
        }

        /**
         * FrameLenBuf读取FrameLength
         * */
        while (frameLenBuf.readableBytes() < LENGTH_SIZE) {
            ByteBuf next = buffers.getFirst();
            int toRead = Math.min(next.readableBytes(), LENGTH_SIZE - frameLenBuf.readableBytes());
            frameLenBuf.writeBytes(next, toRead);
            if (!next.isReadable()) {
                buffers.removeFirst().readableBytes();
            }
        }

        nextFrameSize = frameLenBuf.readableBytes() - LENGTH_SIZE;
        totalSize -= nextFrameSize;
        frameLenBuf.clear();
        return nextFrameSize;
    }

    public interface Interceptor {

        /**
         * Handles data received from the remote end.
         *
         * @param data Buffer containing data.
         * @return "true" if the interceptor expects more data, "false" to uninstall the interceptor.
         */
        boolean handle(ByteBuf data) throws Exception;

        /** Called if an exception is thrown in the channel pipeline. */
        void exceptionCaught(Throwable cause) throws Exception;

        /** Called if the channel is closed and the interceptor is still installed. */
        void channelInactive() throws Exception;

    }

}
