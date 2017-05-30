package com.sdu.spark.serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * @author hanhan.zhang
 * */
public class JavaDeserializationStream extends DeserializationStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaDeserializationStream.class);

    private ObjectInputStream objIn;

    public JavaDeserializationStream(InputStream is, ClassLoader loader) throws IOException{
        this.objIn = new ObjectInputStream(is) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                return Class.forName(desc.getName(), false, loader);
            }
        };
    }

    @Override
    public <T> T readObject() {
        try {
            return (T) this.objIn.readObject();
        } catch (IOException e) {
            LOGGER.error("java stream deserialize object exception", e);
        } catch (ClassNotFoundException e) {
            LOGGER.error("java deserialize object class not found", e);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        this.objIn.close();
    }
}
