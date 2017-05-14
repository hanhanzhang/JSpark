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
     * 编码报文头部[即将报文头部信息写入ByteBuf]
     * */
    void encode(ByteBuf buf);
}
