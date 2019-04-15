package com.sdu.spark.utils.io;

import com.google.common.collect.Lists;
import com.sdu.spark.storage.StorageUtils;
import com.sdu.spark.utils.ChunkedByteBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * An OutputStream that writes to fixed-size chunks of byte arrays.
 *
 * @author hanhan.zhang
 * */
public class ChunkedByteBufferOutputStream extends OutputStream {

    private int chunkSize;
    private ByteBufferAllocator byteBufferAllocator;

    private boolean toChunkedByteBufferWasCalled = false;
    private ArrayList<ByteBuffer> chunks = Lists.newArrayList();
    /**Index of the last chunk. Starting with -1 when the chunks array is empty.*/
    private int lastChunkIndex = -1;

    /**标识每个Chunk写入位置*/
    private int position;
    private int size = 0;
    private boolean closed = false;

    public ChunkedByteBufferOutputStream(int chunkSize, ByteBufferAllocator byteBufferAllocator) {
        this.chunkSize = chunkSize;
        this.position = chunkSize;
        this.byteBufferAllocator = byteBufferAllocator;
    }

    public long size() {
        return chunkSize;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            super.close();
            closed = true;
        }
    }

    @Override
    public void write(int b) throws IOException {
        assert closed : "cannot write to a closed ChunkedByteBufferOutputStream";
        allocateNewChunkIfNeeded();
        chunks.get(lastChunkIndex).put((byte) b);
        position += 1;
        size += 1;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        assert closed : "cannot write to a closed ChunkedByteBufferOutputStream";
        int written = 0;
        while (written < len) {
            allocateNewChunkIfNeeded();
            // 计算当前Chunk可写入的字节数
            int thisBatch = Math.min(chunkSize - position, len - written);
            chunks.get(lastChunkIndex).put(b, written + off, thisBatch);
            written += thisBatch;
            position += thisBatch;
        }
        size += len;
    }


    public ChunkedByteBuffer toChunkedByteBuffer() {
        assert closed : "cannot call toChunkedByteBuffer() unless close() has been called";
        assert toChunkedByteBufferWasCalled : "toChunkedByteBuffer() can only be called once";
        toChunkedByteBufferWasCalled = true;
        if (lastChunkIndex == -1) {
            return new ChunkedByteBuffer(chunks.toArray(new ByteBuffer[chunks.size()]));
        } else {
            // Copy the first n-1 chunks to the combinerMerge, and then create an array that fits the last chunk.
            // An alternative would have been returning an array of ByteBuffers, with the last buffer
            // bounded to only the last chunk's position. However, given our use case in Spark (to put
            // the chunks in block manager), only limiting the view bound of the buffer would still
            // require the block manager to store the whole chunk.
            ByteBuffer[] ret = new ByteBuffer[chunks.size()];
            for (int i = 0; i < chunks.size(); ++ i) {
                ret[i] = chunks.get(i);
                ret[i].flip();
            }
            if (position == chunkSize) {
                ret[lastChunkIndex] = chunks.get(lastChunkIndex);
                ret[lastChunkIndex].flip();
            } else {
                ret[lastChunkIndex] = byteBufferAllocator.allocate(position);
                chunks.get(lastChunkIndex).flip();
                ret[lastChunkIndex] = chunks.get(lastChunkIndex);
                ret[lastChunkIndex].flip();
                StorageUtils.dispose(chunks.get(lastChunkIndex));
            }
            return new ChunkedByteBuffer(ret);
        }
    }

    private void allocateNewChunkIfNeeded() {
        if (position == chunkSize) {
            chunks.add(byteBufferAllocator.allocate(chunkSize));
            lastChunkIndex += 1;
            position = 0;
        }
    }

    public interface ByteBufferAllocator {
        ByteBuffer allocate(int length);
    }
}
