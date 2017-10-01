package com.sdu.spark.serializer;

import com.sdu.spark.storage.BlockId;
import com.sdu.spark.utils.ChunkedByteBuffer;

import java.io.InputStream;
import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public class SerializerManager {

    public ChunkedByteBuffer dataSerializeWithExplicitClassTag(BlockId blockId, Iterator<?> values) {
        throw new UnsupportedOperationException("");
    }

    public <T> Iterator<T> dataDeserializeStream(BlockId blockId, InputStream inputStream) {
        throw new UnsupportedOperationException("");
    }
}
