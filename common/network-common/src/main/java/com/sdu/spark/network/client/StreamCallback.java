package com.sdu.spark.network.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public interface StreamCallback {
    /** Called upon receipt of stream data. */
    void onData(String streamId, ByteBuffer buf) throws IOException;

    /** Called when all data from the stream has been received. */
    void onComplete(String streamId) throws IOException;

    /** Called if there's an error reading data from the stream. */
    void onFailure(String streamId, Throwable cause) throws IOException;
}
