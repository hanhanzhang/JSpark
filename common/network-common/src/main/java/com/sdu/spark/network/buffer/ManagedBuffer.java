package com.sdu.spark.network.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public abstract class ManagedBuffer {

    public abstract long size();

    /**
     * 转为Netty Object用于网络数据输出, Object可为:
     * 1: {@link io.netty.buffer.ByteBuf}
     * 2: {@link io.netty.channel.FileRegion}
     * */
    public abstract Object convertToNetty() throws IOException;

    public abstract ByteBuffer nioByteBuffer() throws IOException;

    public abstract void release();

}
