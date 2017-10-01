package com.sdu.spark.network.buffer;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
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
    public ManagedBuffer release() {
        return this;
    }

    @Override
    public ManagedBuffer retain() {
        return this;
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return new ByteBufInputStream(Unpooled.wrappedBuffer(buf));
    }
}
