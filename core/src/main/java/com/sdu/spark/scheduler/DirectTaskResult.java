package com.sdu.spark.scheduler;

import com.sdu.spark.utils.Utils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class DirectTaskResult<T> implements TaskResult<T>, Externalizable {

    public ByteBuffer valueBytes;

    public DirectTaskResult(ByteBuffer valueBytes) {
        this.valueBytes = valueBytes;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(valueBytes.remaining());
        Utils.writeByteBuffer(valueBytes, out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int byteLength = in.readInt();
        byte[] byteVal = new byte[byteLength];
        in.readFully(byteVal);
        valueBytes = ByteBuffer.wrap(byteVal);
    }
}
