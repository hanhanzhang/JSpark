package com.sdu.spark.unfase.array;

import com.sdu.spark.unfase.Platform;
import com.sdu.spark.unfase.memory.MemoryBlock;

/**
 * @author hanhan.zhang
 * */
public class LongArray {

    private static final long WIDTH = 8;

    private final MemoryBlock memory;
    private final Object baseObj;
    private final long baseOffset;

    private final long length;

    public LongArray(MemoryBlock memory) {
        assert memory.size() < (long) Integer.MAX_VALUE * 8: "Array size > 4 billion elements";
        this.memory = memory;
        this.baseObj = this.memory.getBaseObject();
        this.baseOffset = this.memory.getBaseOffset();
        this.length = this.memory.size() / WIDTH;
    }

    public MemoryBlock memoryBlock() {
        return memory;
    }

    public Object getBaseObject() {
        return baseObj;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long size() {
        return length;
    }

    public void zeroOut() {
        for (long off = baseOffset; off < baseOffset + length * WIDTH; off += WIDTH) {
            Platform.putLong(baseObj, off, 0);
        }
    }

    public void set(int index, long value) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < length : "index (" + index + ") should < length (" + length + ")";
        Platform.putLong(baseObj, baseOffset + index * WIDTH, value);
    }

    public long get(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < length : "index (" + index + ") should < length (" + length + ")";
        return Platform.getLong(baseObj, baseOffset + index * WIDTH);
    }
}
