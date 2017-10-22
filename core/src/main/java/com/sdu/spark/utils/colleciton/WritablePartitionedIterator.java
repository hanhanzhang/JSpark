package com.sdu.spark.utils.colleciton;

import com.sdu.spark.storage.DiskBlockObjectWriter;

/**
 * @author hanhan.zhang
 * */
public interface WritablePartitionedIterator {

    void writeNext(DiskBlockObjectWriter writer);

    boolean hasNext();

    int nextPartition();

}
