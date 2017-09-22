package com.sdu.spark.network.protocol;

import com.google.common.base.Objects;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * 服务端对客户端请求响应
 *
 * @author hanhan.zhang
 * */
public class RpcResponse extends AbstractResponseMessage {

    public final long requestId;

    public RpcResponse(long requestId, ManagedBuffer message) {
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

    @Override
    public Type type() {
        return Type.RpcResponse;
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return null;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        RpcResponse that = (RpcResponse) object;

        return requestId == that.requestId;

    }

    @Override
    public int hashCode() {
        return Objects.hashCode(requestId, body());
    }

    public static RpcResponse decode(ByteBuf buf) {
        long requestId = buf.readLong();
        buf.readInt();
        return new RpcResponse(requestId, new NettyManagedBuffer(buf.retain()));
    }
}
