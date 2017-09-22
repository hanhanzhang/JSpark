package com.sdu.spark.network.protocol;

import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * 单向消息[即接收请求消息并不响应]
 *
 * @author hanhan.zhang
 * */
public class OneWayMessage extends AbstractMessage implements RequestMessage {

    public OneWayMessage(ManagedBuffer body) {
        super(body, true);
    }

    @Override
    public int encodedLength() {
        return 4;
    }

    @Override
    public Type type() {
        return Type.OneWayMessage;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeInt((int) body().size());
    }

    public static OneWayMessage decode(ByteBuf buf) {
        buf.readInt();
        return new OneWayMessage(new NettyManagedBuffer(buf.retain()));
    }

}
