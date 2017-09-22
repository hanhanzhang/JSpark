package com.sdu.spark.network.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public final class StreamRequest extends AbstractMessage implements RequestMessage {

    public final String streamId;

    public StreamRequest(String streamId) {
        this.streamId = streamId;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(streamId);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, streamId);
    }

    public static StreamRequest decode(ByteBuf buf) {
        return new StreamRequest(Encoders.Strings.decode(buf));
    }

    @Override
    public Type type() {
        return Type.StreamRequest;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamId);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof StreamRequest) {
            StreamRequest o = (StreamRequest) other;
            return streamId.equals(o.streamId);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("streamId", streamId)
                .toString();
    }
}
