package com.sdu.spark.storage.memory;

import com.sdu.spark.memory.MemoryMode;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface MemoryEntry extends Serializable {
    long size();
    MemoryMode memoryMode();
}
