package com.sdu.spark.network.client;

import com.sdu.spark.network.buffer.ManagedBuffer;

/**
 * @author hanhan.zhang
 * */
public interface ChunkReceivedCallback {

    /**
     * Called upon receipt of a particular chunk.
     *
     * The given buffer will initially have a refcount of 1, but will be release()'d as soon as this
     * call returns. You must therefore either retain() the buffer or copy its contents before
     * returning.
     */
    void onSuccess(int chunkIndex, ManagedBuffer buffer);

    /**
     * Called upon failure to fetch a particular chunk. Note that this may actually be called due
     * to failure to fetch a prior chunk in this stream.
     *
     * After receiving a failure, the stream may or may not be valid. The client should not assume
     * that the server's side of the stream has been closed.
     */
    void onFailure(int chunkIndex, Throwable e);
}
