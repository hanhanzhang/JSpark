package com.sdu.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class UploadBlock extends BlockTransferMessage {

    public int encodedLength() {
        return 0;
    }

    public void encode(ByteBuf buf) {

    }

    protected Type type() {
        return Type.UPLOAD_BLOCK;
    }

    public static UploadBlock decode(ByteBuf buf) {
        return null;
    }
}
