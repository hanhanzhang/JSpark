package com.sdu.spark.network.shuffle.protocol;

import com.sdu.spark.network.protocol.Encodable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public abstract class BlockTransferMessage implements Encodable{

    protected abstract Type type();

    public enum Type {
        OPEN_BLOCKS(0), UPLOAD_BLOCK(1), REGISTER_EXECUTOR(2), STREAM_HANDLE(3), REGISTER_DRIVER(4),
        HEARTBEAT(5);

        private final byte id;

        Type(int id) {
            assert id < 128 : "Cannot have more than 128 message types";
            this.id = (byte) id;
        }

        public byte id() {
            return id;
        }
    }

    public static class Decoder {
        public static BlockTransferMessage fromByteBuffer(ByteBuffer msg) {
            ByteBuf buf = Unpooled.wrappedBuffer(msg);
            byte type = buf.readByte();
            switch (type) {
                case 0: return OpenBlocks.decode(buf);
                case 1: return UploadBlock.decode(buf);
                case 2: return RegisterExecutor.decode(buf);
                case 3: return StreamHandle.decode(buf);
                case 4: return RegisterDriver.decode(buf);
                case 5: return ShuffleServiceHeartbeat.decode(buf);
                default: throw new IllegalArgumentException("Unknown message type: " + type);
            }
        }
    }

    public ByteBuffer toByteBuffer() {
        // Allow room for encoded message, plus the type byte
        ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
        buf.writeByte(type().id);
        encode(buf);
        assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
        return buf.nioBuffer();
    }

}
