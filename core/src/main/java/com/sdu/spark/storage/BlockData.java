package com.sdu.spark.storage;

import com.sdu.spark.utils.ChunkedByteBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public interface BlockData {

    InputStream toInputStream();

    /**
     * Returns a Netty-friendly wrapper for the block's data.
     *
     * Please see `ManagedBuffer.convertToNetty()` for more details.
     */
    Object toNetty();

    ChunkedByteBuffer toChunkedByteBuffer(Allocator allocator);

    ByteBuffer toByteBuffer();

    long size();

    /**
     * 释放内存, 防止内存泄漏
     * */
    void dispose();

    interface Allocator {
        ByteBuffer allocBuf(int size);
    }

    class ByteBufferBlockData implements BlockData {
        public ChunkedByteBuffer buffer;
        public boolean shouldDispose;

        public ByteBufferBlockData(ChunkedByteBuffer buffer, boolean shouldDispose) {
            this.buffer = buffer;
            this.shouldDispose = shouldDispose;
        }

        @Override
        public InputStream toInputStream() {
            return buffer.toInputStream(shouldDispose);
        }

        @Override
        public Object toNetty() {
            return buffer.toNetty();
        }

        @Override
        public ChunkedByteBuffer toChunkedByteBuffer(Allocator allocator) {
            return buffer.copy(allocator);
        }

        @Override
        public ByteBuffer toByteBuffer() {
            try {
                return buffer.toByteBuffer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long size() {
            return buffer.size();
        }

        @Override
        public void dispose() {
            buffer.dispose();
        }
    }
}
