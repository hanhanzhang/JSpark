package com.sdu.spark.utils;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import com.sdu.spark.network.utils.ByteArrayWritableChannel;
import com.sdu.spark.storage.BlockData;
import com.sdu.spark.storage.StorageUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public class ChunkedByteBuffer {

    private ByteBuffer[] chunks;
    // 释放内存
    private boolean disposed = false;

    public ChunkedByteBuffer(ByteBuffer buffer) {
        this(new ByteBuffer[]{buffer});
    }

    public ChunkedByteBuffer(ByteBuffer[] chunks) {
        Preconditions.checkArgument(chunks != null, "chunks must not be null");
        for (ByteBuffer buf : chunks) {
            Preconditions.checkArgument(buf.position() == 0, "chunks' positions must be 0");
        }
        this.chunks = chunks;
    }

    public long size() {
        long sum = 0L;
        for (ByteBuffer buf : chunks) {
            // position = 0
            sum += buf.limit();
        }
        return sum;
    }

    public void writeFully(WritableByteChannel channel) throws IOException {
        for (ByteBuffer buf : chunks) {
            while (buf.remaining() > 0) {
                channel.write(buf);
            }
        }
    }

    /**
     * Wrap this buffer to view it as a Netty ByteBuf.
     */
    public ByteBuf toNetty() {
        return Unpooled.wrappedBuffer(chunks.length, chunks);
    }

    /**
     * Copy this buffer into a new byte array.
     *
     * @throws UnsupportedOperationException if this buffer's size exceeds the maximum array size.
     */
    public byte[] toArray() throws IOException {
        if (size() >= Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(String.format("cannot call toArray because buffer size (%d bytes) exceeds maximum array size", size()));
        }
        ByteArrayWritableChannel byteChannel = new ByteArrayWritableChannel((int) size());
        writeFully(byteChannel);
        byteChannel.close();
        return byteChannel.getData();
    }

    /**
     * Convert this buffer to a ByteBuffer. If this buffer is backed by a single chunk, its underlying
     * data will not be copied. Instead, it will be duplicated. If this buffer is backed by multiple
     * chunks, the data underlying this buffer will be copied into a new byte buffer. As a result, it
     * is suggested to use this method only if the caller does not need to manage the memory
     * underlying this buffer.
     *
     * @throws UnsupportedOperationException if this buffer's size exceeds the max ByteBuffer size.
     */
    public ByteBuffer toByteBuffer() throws IOException {
        if (chunks.length == 1) {
            return chunks[0].duplicate();
        } else {
            return ByteBuffer.wrap(toArray());
        }
    }

    /**
     * Creates an input stream to read data from this ChunkedByteBuffer.
     *
     * @param dispose if true, [[dispose()]] will be called at the end of the stream
     *                in order to close any memory-mapped files which back this buffer.
     */
    public InputStream toInputStream(boolean dispose) {
        return new ChunkedByteBufferInputStream(this, dispose);
    }

    public InputStream toInputStream() {
        return toInputStream(false);
    }

    /**
     * Make a copy of this ChunkedByteBuffer, copying all of the backing data into new buffers.
     * The new buffer will share no resources with the original buffer.
     *
     * @param allocator a method for allocating byte buffers
     */
    public ChunkedByteBuffer copy(BlockData.Allocator allocator) {
        ByteBuffer[] copiedChunks = new ByteBuffer[chunks.length];
        for (int i = 0; i < chunks.length; ++i) {
            ByteBuffer newChunk = allocator.allocBuf(chunks[i].limit());
            newChunk.put(chunks[i]);
            newChunk.flip();
            copiedChunks[i] = newChunk;
        }
        return new ChunkedByteBuffer(copiedChunks);
    }

    /**
     * Attempt to clean up any ByteBuffer in this ChunkedByteBuffer which is direct or memory-mapped.
     * See [[StorageUtils.dispose]] for more information.
     */
    public void dispose(){
        if (!disposed) {
            for (ByteBuffer buf : chunks) {
                // DirectBuffer回收, 防止内存泄漏
                StorageUtils.dispose(buf);
            }
            disposed = true;
        }
    }

    private class ChunkedByteBufferInputStream extends InputStream {

        private ChunkedByteBuffer chunkedByteBuffer;
        private boolean dispose;

        private Iterator<ByteBuffer> chunks;
        private ByteBuffer currentChunk;

        ChunkedByteBufferInputStream(ChunkedByteBuffer chunkedByteBuffer, boolean dispose) {
            chunks = Arrays.asList(chunkedByteBuffer.chunks).iterator();
            if (chunks.hasNext()) {
                currentChunk = chunks.next();
            }
            this.chunkedByteBuffer = chunkedByteBuffer;
            this.dispose = dispose;
        }

        @Override
        public int read() throws IOException {
            if (currentChunk != null && !currentChunk.hasRemaining() && chunks.hasNext()) {
                currentChunk = chunks.next();
            }
            if (currentChunk != null && currentChunk.hasRemaining()) {
                return UnsignedBytes.toInt(currentChunk.get());
            }
            close();
            return -1;
        }

        @Override
        public int read(byte[] dest, int offset, int length) {
            if (currentChunk != null && !currentChunk.hasRemaining() && chunks.hasNext()) {
                currentChunk = chunks.next();
            }
            if (currentChunk != null && currentChunk.hasRemaining()) {
                int amountToGet = Math.min(currentChunk.remaining(), length);
                currentChunk.get(dest, offset, amountToGet);
                return amountToGet;
            }
            close();
            return -1;
        }

        @Override
        public long skip(long bytes) {
            if (currentChunk != null) {
                int amountToSkip = (int) Math.min(bytes, currentChunk.remaining());
                currentChunk.position(currentChunk.position() + amountToSkip);
                if (currentChunk.remaining() == 0) {
                    if (chunks.hasNext()) {
                        currentChunk = chunks.next();
                    } else {
                        close();
                    }
                }
                return amountToSkip;
            }
            return 0L;
        }

        @Override
        public void close() {
            if (chunkedByteBuffer != null && dispose) {
                chunkedByteBuffer.dispose();
            }
            chunkedByteBuffer = null;
            chunks = null;
            currentChunk = null;
        }
    }
}
