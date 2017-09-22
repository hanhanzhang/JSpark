package com.sdu.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class RegisterExecutor extends BlockTransferMessage {
    public int encodedLength() {
        return 0;
    }

    public void encode(ByteBuf buf) {

    }

    protected Type type() {
        return Type.REGISTER_EXECUTOR;
    }

    public static RegisterExecutor decode(ByteBuf buf) {
        throw new UnsupportedOperationException("");
    }
}
