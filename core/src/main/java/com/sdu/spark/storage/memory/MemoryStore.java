package com.sdu.spark.storage.memory;

import com.sdu.spark.memory.MemoryMode;
import com.sdu.spark.storage.BlockId;

/**
 * Block数据落地JVM内存
 *
 * @author hanhan.zhang
 * */
public class MemoryStore {

    public long evictBlocksToFreeSpace(BlockId blockId, long space, MemoryMode memoryMode) {
        throw new UnsupportedOperationException("");
    }

}
