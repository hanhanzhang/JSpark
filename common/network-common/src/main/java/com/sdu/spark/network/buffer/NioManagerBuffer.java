package com.sdu.spark.network.buffer;

import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class NioManagerBuffer extends ManagedBuffer {

    private ByteBuffer buf;

    public NioManagerBuffer(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public long size() {
        return buf.remaining();
    }

    @Override
    public Object convertToNetty() throws IOException {
        return Unpooled.wrappedBuffer(buf);
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return buf.duplicate();
    }

    @Override
    public void release() {

    }
}
