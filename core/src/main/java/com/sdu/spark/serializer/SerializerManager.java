package com.sdu.spark.serializer;

import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.utils.ChunkedByteBuffer;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public class SerializerManager {

    private Serializer defaultSerializer;
    private SparkConf conf;
    private byte[] encryptionKey;

    public SerializerManager(Serializer defaultSerializer, SparkConf conf) {
        this(defaultSerializer, conf, null);
    }

    public SerializerManager(Serializer defaultSerializer, SparkConf conf, byte[] encryptionKey) {
        this.defaultSerializer = defaultSerializer;
        this.conf = conf;
        this.encryptionKey = encryptionKey;
    }

    public InputStream wrapStream(BlockId blockId, InputStream s) {
        throw new UnsupportedOperationException("");
    }

    public ChunkedByteBuffer dataSerializeWithExplicitClassTag(BlockId blockId, Iterator<?> values) {
        throw new UnsupportedOperationException("");
    }

    public <T> Iterator<T> dataDeserializeStream(BlockId blockId, InputStream inputStream) {
        throw new UnsupportedOperationException("");
    }
}
