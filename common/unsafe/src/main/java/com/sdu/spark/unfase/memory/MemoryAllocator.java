package com.sdu.spark.unfase.memory;

/**
 * @author hanhan.zhang
 * */
public interface MemoryAllocator {

    MemoryAllocator UNSAFE = new UnsafeMemoryAllocator();

    MemoryAllocator HEAP = new HeapMemoryAllocator();

}
