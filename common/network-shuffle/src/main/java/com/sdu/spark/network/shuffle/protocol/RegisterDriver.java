package com.sdu.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class RegisterDriver extends BlockTransferMessage {

    public int encodedLength() {
        return 0;
    }

    public void encode(ByteBuf buf) {

    }

    protected Type type() {
        return Type.REGISTER_DRIVER;
    }

    public static RegisterDriver decode(ByteBuf buf) {
        throw new UnsupportedOperationException("");
    }
}
