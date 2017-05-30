package com.sdu.spark.serializer;

import com.sdu.spark.utils.ByteBufferInputStream;
import com.sdu.spark.utils.ByteBufferOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * 使用JAVA序列化机制
 *
 * @author hanhan.zhang
 * */
public class JavaSerializerInstance implements SerializerInstance {

    /**
     * {@link java.io.ObjectOutputStream}重置阈值
     * */
    private int counterReset;

    private ClassLoader loader;

    public JavaSerializerInstance(int counterReset, ClassLoader loader) {
        this.counterReset = counterReset;
        this.loader = loader;
    }

    @Override
    public <T> ByteBuffer serialize(T object) throws IOException {
        ByteBufferOutputStream bos = new ByteBufferOutputStream();
        SerializationStream out = serializeStream(bos);
        out.writeObject(object);
        out.close();
        return bos.toByteBuffer();
    }

    @Override
    public <T> T deserialize(ByteBuffer buf) throws IOException {
        ByteBufferInputStream bis = new ByteBufferInputStream(buf);
        DeserializationStream in = deserializeStream(bis);
        return in.readObject();
    }

    @Override
    public <T> T deserialize(ByteBuffer buf, ClassLoader loader) throws IOException {
        ByteBufferInputStream bis = new ByteBufferInputStream(buf);
        DeserializationStream in = deserializeStream(bis, loader);
        return in.readObject();
    }

    @Override
    public SerializationStream serializeStream(OutputStream os) throws IOException {
        return new JavaSerializationStream(os, counterReset);
    }

    @Override
    public DeserializationStream deserializeStream(InputStream is) throws IOException {
        return new JavaDeserializationStream(is, loader);
    }

    @Override
    public DeserializationStream deserializeStream(InputStream is, ClassLoader loader) throws IOException {
        return new JavaDeserializationStream(is, loader);
    }
}
