package com.sdu.spark.network.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class ChunkFetchSuccess extends AbstractResponseMessage {

    public final StreamChunkId streamChunkId;

    public ChunkFetchSuccess(StreamChunkId streamChunkId, ManagedBuffer buffer) {
        super(buffer, true);
        this.streamChunkId = streamChunkId;
    }

    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength();
    }

    /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
    }

    public static ChunkFetchSuccess decode(ByteBuf buf) {
        StreamChunkId streamChunkId = StreamChunkId.decode(buf);
        buf.retain();
        NettyManagedBuffer managedBuf = new NettyManagedBuffer(buf.duplicate());
        return new ChunkFetchSuccess(streamChunkId, managedBuf);
    }

    @Override
    public Type type() {
        return Type.ChunkFetchSuccess;
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return new ChunkFetchFailure(streamChunkId, error);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamChunkId, body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess o = (ChunkFetchSuccess) other;
            return streamChunkId.equals(o.streamChunkId) && super.equals(o);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("streamChunkId", streamChunkId)
                .add("buffer", body())
                .toString();
    }
}
