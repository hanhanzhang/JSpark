package com.sdu.spark.serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * @author hanhan.zhang
 * */
public class JavaSerializationStream extends SerializationStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaSerializationStream.class);

    private int counter;
    /**
     * {@link ObjectOutputStream}达到阈值时, Stream重置(丢弃Stream状态, 防止JVM内存溢出)
     * */
    private int counterReset;

    private ObjectOutputStream objOut;

    public JavaSerializationStream(OutputStream out, int counterReset) throws IOException {
        this.counterReset = counterReset;
        this.objOut = new ObjectOutputStream(out);
        this.counter = 0;
    }

    @Override
    public <T> SerializationStream writeObject(T object) {
        try {
            objOut.writeObject(object);
            this.counter++;
            if (this.counterReset > 0 && this.counterReset >= this.counter) {
                // 防止内存溢出
                objOut.reset();
                this.counter = 0;
            }
        } catch (IOException e) {
            LOGGER.error("java serialize object stream exception", e);
        }

        return this;
    }

    @Override
    public void flush() {
        try {
            this.objOut.flush();
        } catch (IOException e) {
            LOGGER.error("java serialize object stream flush exception", e);
        }
    }

    @Override
    public void close() throws IOException {
        this.objOut.close();
    }
}
