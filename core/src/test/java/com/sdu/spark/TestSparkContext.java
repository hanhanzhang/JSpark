package com.sdu.spark;

import org.junit.Test;

/**
 * @author hanhan.zhang
 * */
public class TestSparkContext extends SparkTestUnit {

    @Override
    public void beforeEach() {

    }

    @Test
    public void testOnlySparkContext() {
        conf.setAppName("test").setMaster("local");

        SparkContext sc = new SparkContext(conf);
        SparkEnv beforeEnv = sc.env;

        new SparkContext(conf);

        SparkEnv afterEnv = sc.env;

        assert beforeEnv == afterEnv;
    }

    @Override
    public void afterEach() {

    }

}
