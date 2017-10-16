package com.sdu.spark.shuffle;

import com.sdu.spark.scheduler.MapStatus;
import com.sdu.spark.utils.scala.Product2;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public interface ShuffleWriter<K, V> {

    void write(Iterator<Product2<K, V>> records);

    MapStatus stop(boolean success);

}
