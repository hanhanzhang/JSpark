package com.sdu.spark.serializer;

import java.io.Closeable;

/**
 * @author hanhan.zhang
 * */
public abstract class DeserializationStream implements Closeable {

    public abstract <T> T readObject();

    public <T> T readKey() {
        return readObject();
    }

    public <T> T readValue() {
        return readObject();
    }

}
