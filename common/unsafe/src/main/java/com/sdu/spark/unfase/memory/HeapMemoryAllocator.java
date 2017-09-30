package com.sdu.spark.unfase.memory;

import com.google.common.collect.Maps;
import com.sdu.spark.unfase.Platform;

import java.lang.ref.WeakReference;
import java.util.LinkedList;
import java.util.Map;

/**
 * 分配堆内存
 *
 * @author hanhan.zhang
 * */
public class HeapMemoryAllocator implements MemoryAllocator {

    // 最小内存块阈值
    private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

    // key = 内存块尺寸, value = 内存块集合
    private Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize = Maps.newHashMap();


    private boolean shouldPool(long size) {
        return size >= POOLING_THRESHOLD_BYTES;
    }

    @Override
    public MemoryBlock allocate(long size) throws OutOfMemoryError {
        if (shouldPool(size)) {
            synchronized (this) {
                LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
                if (pool != null) {
                    WeakReference<MemoryBlock> blockReference = pool.pop();
                    MemoryBlock memory = blockReference.get();
                    if (memory != null) {
                        assert memory.size() == size;
                        return memory;
                    }
                }
                bufferPoolsBySize.remove(size);
            }
        }

        // 分配MemoryBlock, free时加入bufferPoolsBySize, 依靠GC清空内存
        // 确保申请的内存空间是不低于size的且是8的倍数内存空间
        long []array = new long[(int) (size + 7) / 8];
        MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8L);
        if (MEMORY_DEBUG_FILL_ENABLED) {
            // 标识已使用
            memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
        }
        return memory;
    }

    @Override
    public void free(MemoryBlock memory) {
        long size = memory.size();
        if (MEMORY_DEBUG_FILL_ENABLED) {
            // 标识已释放
            memory.fill(MEMORY_DEBUG_FILL_FREED_VALUE);
        }
        if (shouldPool(size)) {
            // 加入内存池
            LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
            if (pool == null) {
                pool = new LinkedList<>();
                bufferPoolsBySize.put(size, pool);
            }
            pool.add(new WeakReference<>(memory));
        } else {
            // do nothing
        }
    }

}
