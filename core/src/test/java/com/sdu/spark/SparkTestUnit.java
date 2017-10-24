package com.sdu.spark;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author hanhan.zhang
 * */
public class SparkTestUnit {

    @BeforeClass
    public static void startTest() {
        System.out.println("start test");
    }

    @AfterClass
    public static void finishTest() {
        System.out.println("finished test");
    }
}
