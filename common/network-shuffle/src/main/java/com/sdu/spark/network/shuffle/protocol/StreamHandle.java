package com.sdu.spark.network.shuffle.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class StreamHandle extends BlockTransferMessage {

    public final long streamId;
    public final int numChunks;

    public StreamHandle(long streamId, int numChunks) {
        this.streamId = streamId;
        this.numChunks = numChunks;
    }

    public int encodedLength() {
        return 8 + 4;
    }

    public void encode(ByteBuf buf) {
        buf.writeLong(streamId);
        buf.writeInt(numChunks);
    }

    public static StreamHandle decode(ByteBuf buf) {
        long streamId = buf.readLong();
        int numChunks = buf.readInt();
        return new StreamHandle(streamId, numChunks);
    }

    protected Type type() {
        return Type.STREAM_HANDLE;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamId, numChunks);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("streamId", streamId)
                .add("numChunks", numChunks)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof StreamHandle) {
            StreamHandle o = (StreamHandle) other;
            return Objects.equal(streamId, o.streamId)
                    && Objects.equal(numChunks, o.numChunks);
        }
        return false;
    }

}
