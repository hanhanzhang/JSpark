package com.sdu.spark.network.protocol;

import com.sdu.spark.network.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class RpcFailure extends AbstractMessage implements ResponseMessage {

    public final long requestId;

    public final String errorString;

    public RpcFailure(long requestId, String errorString) {
        this.requestId = requestId;
        this.errorString = errorString;
    }

    @Override
    public int encodedLength() {
        return 8 + Encoders.Strings.encodedLength(errorString);
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        Encoders.Strings.encode(buf, errorString);
    }

    @Override
    public Type type() {
        return Type.RpcFailure;
    }

    public static RpcFailure decode(ByteBuf buf) {
        long requestId = buf.readLong();
        String errorString = Encoders.Strings.decode(buf);
        return new RpcFailure(requestId, errorString);
    }
}
