package com.sdu.spark.unfase.memory;

import com.sdu.spark.unfase.Platform;

/**
 *
 * @author hanhan.zhang
 * */
public class MemoryBlock extends MemoryLocation {

    private final long length;

    /**
     * Optional page number; used when this MemoryBlock represents a page allocated by a
     * TaskMemoryManager. This field is public so that it can be modified by the TaskMemoryManager,
     * which lives in a different package.
     */
    public int pageNumber = -1;


    public MemoryBlock(Object obj, long offset, long length) {
        super(obj, offset);
        this.length = length;
    }

    public long size() {
        return length;
    }

    public static MemoryBlock fromLongArray(long[] array) {
        // 数组在内存中偏移量固定, long = 8字节
        return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8);
    }

    public void fill(byte value) {
        Platform.setMemory(obj, offset, length, value);
    }
}
