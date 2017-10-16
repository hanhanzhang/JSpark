package com.sdu.spark.shuffle;

import com.sdu.spark.utils.scala.Product2;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public interface ShuffleReader<K, V> {

    Iterator<Product2<K, V>> read();

}
