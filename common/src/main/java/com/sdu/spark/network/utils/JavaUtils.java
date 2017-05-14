package com.sdu.spark.network.utils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class JavaUtils {

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }

    public static byte[] bufferToArray(ByteBuffer buffer) {
        if (buffer.hasArray() && buffer.arrayOffset() == 0 &&
                buffer.array().length == buffer.remaining()) {
            return buffer.array();
        } else {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
    }
}
