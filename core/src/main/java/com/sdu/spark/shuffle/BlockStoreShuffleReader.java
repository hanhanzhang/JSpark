package com.sdu.spark.shuffle;

import com.sdu.spark.utils.scala.Product2;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
 *
 * @author hanhan.zhang
 * */
public class BlockStoreShuffleReader<K, C> implements ShuffleReader<K, C> {

    @Override
    public Iterator<Product2<K, C>> read() {
        return null;
    }

}
