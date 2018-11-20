package com.sdu.spark.memory;

import com.google.common.collect.Lists;
import com.sdu.spark.SparkTestUnit;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockManagerInfo.*;
import com.sdu.spark.storage.BlockStatus;
import com.sdu.spark.utils.scala.Tuple2;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hanhan.zhang
 * */
public class TestMemoryManager extends SparkTestUnit {

    private List<Tuple2<BlockId, BlockStatus>> evictedBlocks = Lists.newLinkedList();

    private AtomicLong evictBlocksToFreeSpaceCalled = new AtomicLong(0);

    @Override
    public void beforeEach() {
        evictedBlocks.clear();
        evictBlocksToFreeSpaceCalled.set(-1L);
    }

    @Test
    public void testAcquireExecutionMemory() {
        MemoryManager memoryManager = createStaticMemoryManager(1000L);
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(memoryManager, 0);
        TestMemoryConsumer consumer = new TestMemoryConsumer(taskMemoryManager);

        // 申请内存
        assert taskMemoryManager.acquireExecutionMemory(100L, consumer) == 100L;
        assert taskMemoryManager.acquireExecutionMemory(400L, consumer) == 400L;
        assert taskMemoryManager.acquireExecutionMemory(400L, consumer) == 400L;
        assert taskMemoryManager.acquireExecutionMemory(200L, consumer) == 200L;
        assert taskMemoryManager.acquireExecutionMemory(100L, consumer) == 100L;
        assert taskMemoryManager.acquireExecutionMemory(100L, consumer) == 100L;

        // 释放内存
        taskMemoryManager.cleanUpAllAllocatedMemory();

        // 申请内存
        assert taskMemoryManager.acquireExecutionMemory(1000L, consumer) == 1000L;
        assert taskMemoryManager.acquireExecutionMemory(100L, consumer) == 100L;
    }

    @Test
    public void testMultiAcquireExecutionMemory() {
        // 并发请求, 并保证每个Task分配内存范围: 1/N * poolSize < X < 1/2N * maxCapacity
    }

    @Override
    public void afterEach() {

    }

    private MemoryManager createStaticMemoryManager(long memorySize) {
        return new StaticMemoryManager(
                conf,
                Runtime.getRuntime().availableProcessors(),
                memorySize,
                memorySize
        );
    }
}
