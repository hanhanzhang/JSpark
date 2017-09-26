package com.sdu.spark.serializer;

import com.sdu.spark.rpc.SparkConf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author hanhan.zhang
 * */
public class JavaSerializer implements Serializer, Externalizable {

    private int counterReset;
    private boolean extraDebugInfo;

    public JavaSerializer(SparkConf conf) {
        this.counterReset = conf.getInt("spark.serializer.objectStreamReset", 10);
        this.extraDebugInfo = conf.getBoolean("spark.serializer.extraDebugInfo", true);
    }

    @Override
    public SerializerInstance newInstance() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return new JavaSerializerInstance(counterReset, classLoader);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(counterReset);
        out.writeBoolean(extraDebugInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        counterReset = in.readInt();
        extraDebugInfo = in.readBoolean();
    }
}
