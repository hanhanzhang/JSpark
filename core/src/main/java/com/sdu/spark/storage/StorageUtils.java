package com.sdu.spark.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class StorageUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageUtils.class);

    public static void dispose(ByteBuffer buffer) {
        if (buffer != null && buffer instanceof MappedByteBuffer) {
            LOGGER.info("Disposing of {}", buffer);
            cleanDirectBuffer((DirectBuffer) buffer);
        }
    }

    private static void cleanDirectBuffer(DirectBuffer buffer) {
        Cleaner cleaner = buffer.cleaner();
        if (cleaner != null) {
            cleaner.clean();
        }
    }
}
