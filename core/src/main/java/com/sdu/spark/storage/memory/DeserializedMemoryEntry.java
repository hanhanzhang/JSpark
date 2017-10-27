package com.sdu.spark.storage.memory;

import com.sdu.spark.memory.MemoryMode;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class DeserializedMemoryEntry<T> implements MemoryEntry<T> {

    private Class<T> classTag;
    public long size;
    public List<T> array;

    public DeserializedMemoryEntry(long size,
                                   List<T> array,
                                   Class<T> classTag) {
        this.size = size;
        this.array = array;
        this.classTag = classTag;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public MemoryMode memoryMode() {
        return MemoryMode.ON_HEAP;
    }

    @Override
    public Class<T> classTag() {
        return classTag;
    }
}
