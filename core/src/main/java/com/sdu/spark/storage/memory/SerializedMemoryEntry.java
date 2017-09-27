package com.sdu.spark.storage.memory;

import com.sdu.spark.memory.MemoryMode;
import com.sdu.spark.utils.ChunkedByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class SerializedMemoryEntry implements MemoryEntry {

    public ChunkedByteBuffer buffer;
    public MemoryMode memoryMode;

    public SerializedMemoryEntry(ChunkedByteBuffer buffer, MemoryMode memoryMode) {
        this.buffer = buffer;
        this.memoryMode = memoryMode;
    }

    @Override
    public long size() {
        return buffer.size();
    }

    @Override
    public MemoryMode memoryMode() {
        return memoryMode;
    }
}
