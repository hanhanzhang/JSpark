package com.sdu.spark.serializer;

import java.io.Closeable;
import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public abstract class SerializationStream implements Closeable {

    public abstract <T> SerializationStream writeObject(T object);

    public abstract void flush();

    public <T> SerializationStream writeKey(T key) {
        return writeObject(key);
    }

    public <T> SerializationStream writeValue(T value) {
        return writeObject(value);
    }

    public <T> SerializationStream writeAll(Iterator<T> iterator) {
        while (iterator.hasNext()) {
            writeObject(iterator.next());
        }
        return this;
    }

}
