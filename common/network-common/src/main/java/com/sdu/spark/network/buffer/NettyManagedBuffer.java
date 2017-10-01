package com.sdu.spark.network.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class NettyManagedBuffer extends ManagedBuffer {

    private final ByteBuf buf;

    public NettyManagedBuffer(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public long size() {
        return buf.readableBytes();
    }

    @Override
    public Object convertToNetty() throws IOException {
        return buf.duplicate().retain();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return buf.nioBuffer();
    }

    @Override
    public ManagedBuffer release() {
        buf.release();
        return this;
    }

    @Override
    public ManagedBuffer retain() {
        buf.retain();
        return this;
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return new ByteBufInputStream(buf);
    }
}
