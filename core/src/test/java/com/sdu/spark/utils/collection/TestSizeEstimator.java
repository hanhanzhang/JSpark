package com.sdu.spark.utils.collection;

import com.sdu.spark.SparkTestUnit;
import com.sdu.spark.utils.SizeEstimator;
import com.sdu.spark.utils.test.DummyClass1;
import com.sdu.spark.utils.test.DummyClass2;
import org.junit.Before;
import org.junit.Test;

/**
 * @author hanhan.zhang
 * */
public class TestSizeEstimator extends SparkTestUnit {

    @Before
    public void beforeEach() {
        System.setProperty("os.arch", "amd64");
        System.setProperty("spark.test.useCompressedOops", "true");
    }

    @Test
    public void testSimpleClass() {
        // 对象引用 = 16
        long size = SizeEstimator.estimate(new DummyClass1());
        assert size == 16;

        // 16 + 4 + 8
        size = SizeEstimator.estimate(new DummyClass2());
    }

    @Override
    public void afterEach() {

    }
}
