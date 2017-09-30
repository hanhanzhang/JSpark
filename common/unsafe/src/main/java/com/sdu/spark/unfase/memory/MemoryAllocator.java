package com.sdu.spark.unfase.memory;

/**
 * @author hanhan.zhang
 * */
public interface MemoryAllocator {

    boolean MEMORY_DEBUG_FILL_ENABLED = Boolean.parseBoolean(
                            System.getProperty("spark.memory.debugFill", "false"));

    // Same as jemalloc's debug fill values.
    byte MEMORY_DEBUG_FILL_CLEAN_VALUE = (byte)0xa5;
    byte MEMORY_DEBUG_FILL_FREED_VALUE = (byte)0x5a;

    MemoryBlock allocate(long size) throws OutOfMemoryError;

    void free(MemoryBlock memory);

    MemoryAllocator UNSAFE = new UnsafeMemoryAllocator();

    MemoryAllocator HEAP = new HeapMemoryAllocator();

}
