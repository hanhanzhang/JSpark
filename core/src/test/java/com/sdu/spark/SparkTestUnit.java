package com.sdu.spark;

import com.sdu.spark.rpc.SparkConf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * @author hanhan.zhang
 * */
public abstract class SparkTestUnit {

    public static final SparkConf conf;

    static {
        System.setProperty("java.io.tmpdir", "/Users/hanhan.zhang/tmp");

        conf = new SparkConf();

        // driver进程只允许启动一个SparkContext
        conf.set("spark.driver.allowMultipleContexts", "false");

        // Spark Shuffle根目录、子目录数
        conf.set("spark.local.dir", "/Users/hanhan.zhang/tmp");
        conf.set("spark.diskStore.subDirectories", "64");

        // 网络数据传输压缩
        conf.set("spark.broadcast.compress", "true");
        conf.set("spark.shuffle.compress", "true");
        conf.set("spark.rdd.compress", "false");
        conf.set("spark.shuffle.spill.compress", "true");

        // 指定压缩算法(lz4更高效)
        conf.set("spark.io.compression.codec", "lz4");

        // 指定内存池参数
        // StaticMemoryManager仅支持Execution使用非堆内存, UnifiedMemoryManager支持Storage和Execution都使用非堆内存
        conf.set("spark.memory.offHeap.size", "10240");
        // 设置Storage内存占offHeap的比例(仅在UnifiedMemoryManager下生效)
        conf.set("spark.memory.storageFraction", "0.5");
        // Execution/Storage使用内存类型(堆内存、非堆内存)
        conf.set("spark.memory.offHeap.enabled", "false");

    }

    @BeforeClass
    public static void startTest() {
        System.out.println("start test");
    }

    @Before
    public abstract void beforeEach();

    @AfterClass
    public static void finishTest() {
        System.out.println("finished test");
    }

    @After
    public abstract void afterEach();
}
