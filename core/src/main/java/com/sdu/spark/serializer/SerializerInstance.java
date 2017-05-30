package com.sdu.spark.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * 序列化实例
 *
 * @author hanhan.zhang
 * */
public interface SerializerInstance {

    <T> ByteBuffer serialize(T object) throws IOException;

    <T> T deserialize(ByteBuffer buf) throws IOException;

    <T> T deserialize(ByteBuffer buf, ClassLoader loader) throws IOException;

    SerializationStream serializeStream(OutputStream os) throws IOException;

    DeserializationStream deserializeStream(InputStream is) throws IOException;

    DeserializationStream deserializeStream(InputStream is, ClassLoader loader) throws IOException;

}
