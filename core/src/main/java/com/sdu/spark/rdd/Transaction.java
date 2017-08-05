package com.sdu.spark.rdd;

/**
 * @author hanhan.zhang
 * */
public interface Transaction<I, R> {

    R apply(I input);

}
