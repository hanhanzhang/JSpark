package com.sdu.spark.network.shuffle.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.sdu.spark.network.protocol.Encoders;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

/**
 * @author hanhan.zhang
 * */
public class UploadBlock extends BlockTransferMessage {

    public final String appId;
    public final String execId;
    public final String blockId;
    // TODO: StorageLevel is serialized separately in here because StorageLevel is not available in
    // this package. We should avoid this hack.
    public final byte[] metadata;
    public final byte[] blockData;

    public UploadBlock(
            String appId,
            String execId,
            String blockId,
            byte[] metadata,
            byte[] blockData) {
        this.appId = appId;
        this.execId = execId;
        this.blockId = blockId;
        this.metadata = metadata;
        this.blockData = blockData;
    }

    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) +
               Encoders.Strings.encodedLength(execId) +
               Encoders.Strings.encodedLength(blockId) +
               Encoders.ByteArrays.encodedLength(metadata) +
               Encoders.ByteArrays.encodedLength(blockData);
    }

    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        Encoders.Strings.encode(buf, blockId);
        Encoders.ByteArrays.encode(buf, metadata);
        Encoders.ByteArrays.encode(buf, blockData);
    }

    public static UploadBlock decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String blockId = Encoders.Strings.decode(buf);
        byte[] metadata = Encoders.ByteArrays.decode(buf);
        byte[] blockData = Encoders.ByteArrays.decode(buf);
        return new UploadBlock(appId, execId, blockId, metadata, blockData);
    }

    protected Type type() {
        return Type.UPLOAD_BLOCK;
    }

    @Override
    public int hashCode() {
        int objectsHashCode = Objects.hashCode(appId, execId, blockId);
        return (objectsHashCode * 41 + Arrays.hashCode(metadata)) * 41 + Arrays.hashCode(blockData);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("blockId", blockId)
                .add("metadata size", metadata.length)
                .add("block size", blockData.length)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof UploadBlock) {
            UploadBlock o = (UploadBlock) other;
            return Objects.equal(appId, o.appId)
                    && Objects.equal(execId, o.execId)
                    && Objects.equal(blockId, o.blockId)
                    && Arrays.equals(metadata, o.metadata)
                    && Arrays.equals(blockData, o.blockData);
        }
        return false;
    }


}
