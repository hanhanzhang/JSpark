package com.sdu.spark;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.sdu.spark.utils.Utils.nonNegativeMod;

/**
 * @author hanhan.zhang
 * */
public abstract class Partitioner implements Serializable {

    public abstract int numPartitions();
    public abstract int getPartition(Object key);


    public static class HashPartitioner extends Partitioner {

        private int partitions;

        public HashPartitioner(int partitions) {
            checkArgument(partitions > 0, String.format("Number of partitions cannot be negative but found %d", partitions));
            this.partitions = partitions;
        }

        @Override
        public int numPartitions() {
            return partitions;
        }

        @Override
        public int getPartition(Object key) {
            if (key == null) {
                return 0;
            }
            return nonNegativeMod(key.hashCode(), partitions);
        }

    }
}
