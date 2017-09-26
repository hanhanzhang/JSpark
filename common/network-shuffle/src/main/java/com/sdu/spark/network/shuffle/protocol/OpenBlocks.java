package com.sdu.spark.network.shuffle.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.sdu.spark.network.protocol.Encoders;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

/**
 * @author hanhan.zhang
 * */
public class OpenBlocks extends BlockTransferMessage {

    public String appId;
    public String execId;
    public String[] blockIds;

    public OpenBlocks(String appId, String execId, String[] blockIds) {
        this.appId = appId;
        this.execId = execId;
        this.blockIds = blockIds;
    }

    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) +
               Encoders.Strings.encodedLength(execId) +
               Encoders.StringArrays.encodedLength(blockIds);
    }

    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        Encoders.StringArrays.encode(buf, blockIds);
    }

    public static OpenBlocks decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String[] blockIds = Encoders.StringArrays.decode(buf);
        return new OpenBlocks(appId, execId, blockIds);
    }

    protected Type type() {
        return Type.OPEN_BLOCKS;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId) * 41 + Arrays.hashCode(blockIds);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("blockIds", Arrays.toString(blockIds))
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof OpenBlocks) {
            OpenBlocks o = (OpenBlocks) other;
            return Objects.equal(appId, o.appId)
                    && Objects.equal(execId, o.execId)
                    && Arrays.equals(blockIds, o.blockIds);
        }
        return false;
    }

}
