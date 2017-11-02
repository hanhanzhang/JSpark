package com.sdu.spark;

import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.serializer.JavaSerializer;
import com.sdu.spark.serializer.KryoSerializer;
import com.sdu.spark.utils.Utils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static com.sdu.spark.network.utils.NettyUtils.getIpV4;

/**
 * @author hanhan.zhang
 * */
public abstract class SparkTestUnit {

    protected static final SparkConf conf;

    static {
        System.setProperty("java.io.tmpdir", "/Users/hanhan.zhang/tmp");
        conf = createSparkConf(false, false);
    }

    private static SparkConf createSparkConf(boolean loadDefaults, boolean kryo) {
        SparkConf conf = new SparkConf();

        // Spark Master
        conf.set("spark.master", String.format("spark://%s:%d", getIpV4(), 6712));

        // Spark AppName
        conf.set("spark.app.name", "test");

        // Spark Driver配置
        conf.set("spark.driver.host", getIpV4());
        conf.set("spark.driver.bindAddress", getIpV4());
        conf.set("spark.driver.port", "9001");
        conf.set("spark.driver.allowMultipleContexts", "false");

        // 设置Spark序列化
        if (kryo) {
            conf.set("spark.serializer", KryoSerializer.class.getName());
        } else {
            // 默认使用Java序列化框架
            conf.set("spark.serializer.objectStreamReset", "1");
            conf.set("spark.serializer", JavaSerializer.class.getName());
        }

        // Spark Shuffle Manager
        conf.set("spark.shuffle.manager", "sort");
        // Spark Shuffle根目录、子目录数
        conf.set("spark.local.dir", "/Users/hanhan.zhang/tmp");
        conf.set("spark.diskStore.subDirectories", "64");
        // Spark Shuffle Spill数据量
        conf.set("spark.shuffle.spill.batchSize", "10");
        conf.set("spark.shuffle.spill.initialMemoryThreshold", "512");

        // 网络数据传输压缩
        conf.set("spark.broadcast.compress", "true");
        conf.set("spark.shuffle.compress", "true");
        conf.set("spark.rdd.compress", "false");
        conf.set("spark.shuffle.spill.compress", "true");

        // 指定压缩算法(lz4更高效)
        conf.set("spark.io.compression.codec", "lz4");

        // 指定内存池参数
        // StaticMemoryManager仅支持Execution使用非堆内存, UnifiedMemoryManager支持Storage和Execution都使用非堆内存
        // 使用UnifiedMemoryManager
        conf.set("spark.memory.useLegacyMode", "true");
        conf.set("spark.memory.offHeap.size", "10240");
        // 设置Storage内存占offHeap的比例(仅在UnifiedMemoryManager下生效)
        conf.set("spark.memory.storageFraction", "0.5");
        // Execution/Storage使用内存类型(堆内存、非堆内存)
        conf.set("spark.memory.offHeap.enabled", "false");

        return conf;
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
