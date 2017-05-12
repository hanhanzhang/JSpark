package com.sdu.spark.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class ByteBufferOutputStream extends ByteArrayOutputStream {

    private int capacity;

    private boolean closed = false;

    public ByteBufferOutputStream() {
        this(32);
    }

    public ByteBufferOutputStream(int capacity) {
        this.capacity = capacity;
    }

    public int getCount(){
        return count;
    }


    @Override
    public void write(int b) {
        if (!closed) {
            throw new IllegalStateException("cannot write to a closed ByteBufferOutputStream");
        }
        super.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len){
        if (!closed) {
            throw new IllegalStateException("cannot write to a closed ByteBufferOutputStream");
        }
        super.write(b, off, len);
    }

    @Override
    public void reset() {
        if (!closed) {
            throw new IllegalStateException("cannot write to a closed ByteBufferOutputStream");
        }
        super.reset();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            super.close();
            closed = true;
        }
    }

    public ByteBuffer toByteBuffer() {
        if (!closed) {
            throw new IllegalStateException("cannot write to a closed ByteBufferOutputStream");
        }
        return ByteBuffer.wrap(buf, 0, count);
    }


}
