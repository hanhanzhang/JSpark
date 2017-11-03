package com.sdu.spark.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * @author hanhan.zhang
 * */
public class SerializableBuffer implements Serializable {

    public transient ByteBuffer buffer;

    public SerializableBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    private void readObject(ObjectInputStream in) throws IOException{
        int length = in.readInt();
        buffer = ByteBuffer.allocate(length);
        int amountRead = 0;
        ReadableByteChannel channel = Channels.newChannel(in);
        while (amountRead < length) {
            int ret = channel.read(buffer);
            if (ret == -1) {
                throw new EOFException("End of file before fully reading buffer");
            }
            amountRead += ret;
        }
        buffer.rewind(); // Allow us to read it later
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(buffer.limit());
        if (Channels.newChannel(out).write(buffer) != buffer.limit()) {
            throw new IOException("Could not fully write buffer to combiner stream");
        }
        buffer.rewind(); // Allow us to write it again later
    }
}
