package com.sdu.spark.utils;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public abstract class TIterator<T> implements Iterator<T>, Serializable {

    public abstract <B> TIterator<B> map(MapFunction<T, B> func);

    public abstract TIterator<T> filter(FilterFunction<T> func);

    public interface MapFunction<A, B> {
        B map(A data);
    }

    public interface FilterFunction<A> {
        boolean filter(A data);
    }

}
