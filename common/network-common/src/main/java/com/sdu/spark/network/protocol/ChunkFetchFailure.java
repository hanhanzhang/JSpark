package com.sdu.spark.network.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class ChunkFetchFailure extends AbstractMessage implements ResponseMessage {

    public final StreamChunkId streamChunkId;
    public final String errorString;

    public ChunkFetchFailure(StreamChunkId streamChunkId, String errorString) {
        this.streamChunkId = streamChunkId;
        this.errorString = errorString;
    }

    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength() + Encoders.Strings.encodedLength(errorString);
    }

    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
        Encoders.Strings.encode(buf, errorString);
    }

    public static ChunkFetchFailure decode(ByteBuf buf) {
        StreamChunkId streamChunkId = StreamChunkId.decode(buf);
        String errorString = Encoders.Strings.decode(buf);
        return new ChunkFetchFailure(streamChunkId, errorString);
    }

    @Override
    public Type type() {
        return Type.ChunkFetchFailure;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamChunkId, errorString);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ChunkFetchFailure) {
            ChunkFetchFailure o = (ChunkFetchFailure) other;
            return streamChunkId.equals(o.streamChunkId) && errorString.equals(o.errorString);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                            .add("streamChunkId", streamChunkId)
                            .add("errorString", errorString)
                            .toString();
    }
}
