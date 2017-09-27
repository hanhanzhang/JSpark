package com.sdu.spark.utils.colleciton;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public class OpenHashSet<T> implements Serializable {

    private int initialCapacity;
    private double loadFactor;

    public OpenHashSet(int initialCapacity) {
        this(initialCapacity, 0.7);
    }


    public OpenHashSet(int initialCapacity, double loadFactor) {
        this.initialCapacity = initialCapacity;
        this.loadFactor = loadFactor;
    }

    public void add(T k) {
        throw new UnsupportedOperationException("");
    }

    public boolean contains(T k) {
        throw new UnsupportedOperationException("");
    }
}
