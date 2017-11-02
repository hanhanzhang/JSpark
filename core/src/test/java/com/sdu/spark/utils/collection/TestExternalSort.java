package com.sdu.spark.utils.collection;

import com.google.common.collect.Interner;
import com.google.common.collect.Lists;
import com.sdu.spark.*;
import com.sdu.spark.Partitioner.*;
import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.utils.colleciton.ExternalSorter;
import com.sdu.spark.utils.scala.Tuple2;
import org.junit.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class TestExternalSort extends SparkTestUnit {

    @Override
    public void beforeEach() {

    }

    @Test
    public void testFewElementsPerPartition() {
        SparkContext sc = new SparkContext(conf);
        // TaskContext
        TaskContext context = fakeTaskContext(sc.env);
        // 数据聚合运算
        Aggregator<Integer, Integer, Integer> aggregator = new Aggregator<>(
                x -> x, (x, y) -> x + y, (x, y) -> x + y
        );
        // 关键字排序
        Comparator<Integer> comparator = (x, y) -> x - y;

        @SuppressWarnings("unchecked")
        List<Tuple2<Integer, Integer>> elements = Lists.newArrayList(
                new Tuple2<>(1, 1),
                new Tuple2<>(2, 2),
                new Tuple2<>(5, 5),
                new Tuple2<>(6, 6),
                new Tuple2<>(7, 7),
                new Tuple2<>(9, 3)
        );

        // 数据聚合&排序
        ExternalSorter<Integer, Integer, Integer> sorter = new ExternalSorter<>(
                context,
                aggregator,
                new HashPartitioner(7),
                comparator
        );
        sorter.insertAll(elements.iterator());
        sorter.partitionedIterator();

        // 数据聚合
    }

    @Override
    public void afterEach() {

    }

    private static TaskContext fakeTaskContext(SparkEnv env) {
        // 每个Task实例化个TaskMemoryManager
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(env.memoryManager, 0);
        return new TaskContextImpl(
                0,
                0,
                0,
                0,
                taskMemoryManager,
                new Properties()
        );
    }
}
