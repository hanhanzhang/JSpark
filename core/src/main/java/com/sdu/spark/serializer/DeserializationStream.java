package com.sdu.spark.serializer;

import com.sdu.spark.utils.NextIterator;
import com.sdu.spark.utils.scala.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

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


    public Iterator<Tuple2<Object, Object>> asKeyValueIterator() {
        return new NextIterator<Tuple2<Object, Object>>() {
            @Override
            public Tuple2<Object, Object> getNext() {
                try {
                    return new Tuple2<>(readKey(), readValue());
                } catch (Exception e) {
                    finished = true;
                    return null;
                }
            }

            @Override
            public void close() {
                try {
                    DeserializationStream.this.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        };
    }

}
