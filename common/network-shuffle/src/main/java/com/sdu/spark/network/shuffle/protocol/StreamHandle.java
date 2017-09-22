package com.sdu.spark.network.shuffle.protocol;

import com.sun.xml.internal.xsom.impl.parser.DelayedRef;
import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class StreamHandle extends BlockTransferMessage {

    public int encodedLength() {
        return 0;
    }

    public void encode(ByteBuf buf) {

    }

    protected Type type() {
        return Type.STREAM_HANDLE;
    }

    public static StreamHandle decode(ByteBuf buf) {
        throw new UnsupportedOperationException("");
    }
}
