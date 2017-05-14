package com.sdu.spark.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 消息编码
 *
 * @author hanhan.zhang
 * */
public interface Encodable {

    /**
     * 报文长度
     * */
    int encodedLength();

    /**
     * 报文编码
     * */
    void encode(ByteBuf buf);
}
