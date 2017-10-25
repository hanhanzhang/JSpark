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
