package com.sdu.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class OpenBlocks extends BlockTransferMessage {

    public int encodedLength() {
        return 0;
    }

    public void encode(ByteBuf buf) {

    }

    protected Type type() {
        return Type.OPEN_BLOCKS;
    }

    public static OpenBlocks decode(ByteBuf buf) {
        return null;
    }
}
