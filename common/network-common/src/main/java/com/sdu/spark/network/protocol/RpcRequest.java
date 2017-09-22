package com.sdu.spark.network.protocol;

import com.google.common.base.Objects;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 *
 *
 * @author hanhan.zhang
 * */
public class RpcRequest extends AbstractMessage implements RequestMessage {
    /**
     * 映射RpcRequest与RpcResponse关系
     * */
    public final long requestId;

    public RpcRequest(long requestId, ManagedBuffer message) {
        super(message, true);
        this.requestId = requestId;
    }

    @Override
    public int encodedLength() {
        // requestId + body
        return 8 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        buf.writeInt((int) body().size());
    }

    public static RpcRequest decode(ByteBuf buf) {
        // 读取报文RequestId
        long requestId = buf.readLong();
        // 读取报文体长度
        buf.readInt();
        return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
    }

    @Override
    public Type type() {
        return Type.RpcRequest;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        RpcRequest that = (RpcRequest) object;

        return requestId == that.requestId;

    }

    @Override
    public int hashCode() {
        return Objects.hashCode(requestId, body());
    }
}
