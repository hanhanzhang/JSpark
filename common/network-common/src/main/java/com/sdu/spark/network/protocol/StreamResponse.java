package com.sdu.spark.network.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.sdu.spark.network.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class StreamResponse extends AbstractResponseMessage {

    public final String streamId;
    public final long byteCount;

    public StreamResponse(String streamId, long byteCount, ManagedBuffer buffer) {
        super(buffer, false);
        this.streamId = streamId;
        this.byteCount = byteCount;
    }

    @Override
    public int encodedLength() {
        return 8 + Encoders.Strings.encodedLength(streamId);
    }

    @Override
    public Type type() {
        return Type.StreamResponse;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, streamId);
        buf.writeLong(byteCount);
    }

    public static StreamResponse decode(ByteBuf buf) {
        String streamId = Encoders.Strings.decode(buf);
        long byteCount = buf.readLong();
        return new StreamResponse(streamId, byteCount, null);
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return new StreamFailure(streamId, error);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(byteCount, streamId, body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof StreamResponse) {
            StreamResponse o = (StreamResponse) other;
            return byteCount == o.byteCount && streamId.equals(o.streamId);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("streamId", streamId)
                .add("byteCount", byteCount)
                .add("body", body())
                .toString();
    }
}
