package com.sdu.spark.network.sasl;

import com.sdu.spark.network.buffer.NettyManagedBuffer;
import com.sdu.spark.network.protocol.AbstractMessage;
import com.sdu.spark.network.protocol.Encoders;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author hanhan.zhang
 * */
public class SaslMessage extends AbstractMessage {

    private static final byte TAG_BYTE = (byte) 0xEA;

    public String appId;

    public SaslMessage(String appId, byte[] message) {
        this(appId, Unpooled.wrappedBuffer(message));
    }

    public SaslMessage(String appId, ByteBuf message) {
        super(new NettyManagedBuffer(message), true);
        this.appId = appId;
    }

    @Override
    public int encodedLength() {
        return 1 + Encoders.Strings.encodedLength(appId) + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeByte(TAG_BYTE);
        Encoders.Strings.encode(buf, appId);
        // See comment in encodedLength().
        buf.writeInt((int) body().size());
    }

    @Override
    public Type type() {
        return Type.User;
    }

    public static SaslMessage decode(ByteBuf buf) {
        if (buf.readByte() != TAG_BYTE) {
            throw new IllegalStateException("Expected SaslMessage, received something else"
                    + " (maybe your client does not have SASL enabled?)");
        }

        String appId = Encoders.Strings.decode(buf);
        // See comment in encodedLength().
        buf.readInt();
        return new SaslMessage(appId, buf.retain());
    }
}
