package com.sdu.spark.memory;

import com.sdu.spark.SparkTestUnit;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hanhan.zhang
 * */
public class TestTaskMemoryManager extends SparkTestUnit {

    @Override
    public void beforeEach() {

    }

    @Test
    public void leakedPageMemoryIsDetected() {
        TaskMemoryManager manager = new TaskMemoryManager(
                new StaticMemoryManager(
                        conf,
                        Runtime.getRuntime().availableProcessors(),
                        Long.MAX_VALUE,
                        Long.MAX_VALUE
                ),
                1
        );

        final TestMemoryConsumer consumer = new TestMemoryConsumer(manager);
        manager.allocatePage(4096, consumer);
        // 断言当前Task占用内存量 = 4096
        Assert.assertEquals(4096, manager.getMemoryConsumptionForThisTask());
        Assert.assertEquals(4096, manager.cleanUpAllAllocatedMemory());
    }

    @Override
    public void afterEach() {

    }
}
