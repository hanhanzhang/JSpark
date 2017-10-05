package com.sdu.spark.storage.memory;

import com.sdu.spark.memory.MemoryMode;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class DeserializedMemoryEntry<T> implements MemoryEntry {

    public long size;
    public List<T> array;

    public DeserializedMemoryEntry(long size, List<T> array) {
        this.size = size;
        this.array = array;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public MemoryMode memoryMode() {
        return MemoryMode.ON_HEAP;
    }
}
