package com.sdu.spark.storage;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author hanhan.zhang
 * */
public class DiskBlockObjectWriter extends OutputStream {

    public void write(Object key, Object value) {
        throw new UnsupportedOperationException("");
    }

    public FileSegment commitAndGet() {
        throw new UnsupportedOperationException("");
    }

    public void revertPartialWritesAndClose() {

    }

    @Override
    public void write(int b) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
