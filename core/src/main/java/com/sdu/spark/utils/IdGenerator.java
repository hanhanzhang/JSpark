package com.sdu.spark.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author hanhan.zhang
 * */
public class IdGenerator {

    private AtomicInteger id = new AtomicInteger();

    public int next() {
        return id.incrementAndGet();
    }

}
