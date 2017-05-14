package com.sdu.spark.network.buffer;

import io.netty.buffer.ByteBuf;

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
}
