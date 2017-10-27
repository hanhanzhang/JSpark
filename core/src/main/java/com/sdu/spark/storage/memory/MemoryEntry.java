package com.sdu.spark.storage.memory;

import com.sdu.spark.memory.MemoryMode;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface MemoryEntry<T> extends Serializable {
    long size();
    MemoryMode memoryMode();
    Class<T> classTag();
}
