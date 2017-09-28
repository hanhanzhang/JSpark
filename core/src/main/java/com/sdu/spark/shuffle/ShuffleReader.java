package com.sdu.spark.shuffle;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public interface ShuffleReader<K, V> {

    Iterator<Pair<K, V>> read();

}
