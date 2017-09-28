package com.sdu.spark.shuffle;

import com.sdu.spark.scheduler.MapStatus;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public interface ShuffleWriter<K, V> {

    void write(Iterator<Pair<K, V>> records);

    MapStatus stop(boolean success);

}
