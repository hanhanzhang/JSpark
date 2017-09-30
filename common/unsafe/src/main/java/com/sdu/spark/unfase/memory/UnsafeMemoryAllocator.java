package com.sdu.spark.unfase.memory;

import com.sdu.spark.unfase.Platform;

/**
 * 分配直接内存
 *
 * @author hanhan.zhang
 * */
public class UnsafeMemoryAllocator implements MemoryAllocator {

    @Override
    public MemoryBlock allocate(long size) throws OutOfMemoryError {
        long offset = Platform.allocateMemory(size);
        MemoryBlock memoryBlock = new MemoryBlock(null, offset, size);
        if (MEMORY_DEBUG_FILL_ENABLED) {
            // 标记已使用
            memoryBlock.fill(MEMORY_DEBUG_FILL_CLEAN_VALUE);
        }
        return memoryBlock;
    }

    @Override
    public void free(MemoryBlock memory) {
        assert (memory.obj == null) :
                "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
        if (MEMORY_DEBUG_FILL_ENABLED) {
            // 标记已释放
            memory.fill(MEMORY_DEBUG_FILL_FREED_VALUE);
        }
        Platform.freeMemory(memory.offset);
    }
}
