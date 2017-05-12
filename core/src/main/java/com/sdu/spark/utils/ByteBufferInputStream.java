package com.sdu.spark.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class ByteBufferInputStream extends InputStream {

    private ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() {
        if (buffer == null || buffer.remaining() == 0) {
            cleanUp();
            return -1;
        } else {
            return buffer.get() & 0xFF;
        }
    }

    @Override
    public int read(byte []dest) {
        return read(dest, 0, dest.length);
    }

    @Override
    public int read(byte []dest, int offset, int length){
        if (buffer == null || buffer.remaining() == 0) {
            cleanUp();
            return -1;
        } else {
            int amountToGet = Math.min(buffer.remaining(), length);
            buffer.get(dest, offset, amountToGet);
            return amountToGet;
        }
    }

    @Override
    public long skip(long bytes) throws IOException {
        if (buffer != null) {
            int amountToSkip = Math.min((int) bytes, buffer.remaining());
            buffer.position(buffer.position() + amountToSkip);
            if (buffer.remaining() == 0) {
                cleanUp();
            }
            return (long) amountToSkip;
        } else {
            return 0L;
        }
    }

    /**
     * Clean up the buffer, and potentially dispose of it using StorageUtils.dispose().
     */
    private void cleanUp() {
        if (buffer != null) {
            buffer = null;
        }
    }
}
