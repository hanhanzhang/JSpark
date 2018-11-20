package com.sdu.spark.utils.colleciton;

import com.sdu.spark.utils.scala.Tuple2;

import java.util.Comparator;

public class WritablePartitionedPairCollections {

    public static <K> Comparator<Tuple2<Integer, K>> partitionComparator() {
        return new PartitionAscComparator<>();
    }

    public static <K> Comparator<Tuple2<Integer, K>> partitionKeyComparator(Comparator<K> keyComparator) {
        return new PartitionAndKeyComparator<>(keyComparator);
    }

    /**
     * A comparator for (Int, K) pairs that orders them by only their partition ID
     * */
    private static class PartitionAscComparator<K> implements Comparator<Tuple2<Integer, K>> {

        @Override
        public int compare(Tuple2<Integer, K> a, Tuple2<Integer, K> b) {
            return a._1() - b._1();
        }

    }

    /**
     * A comparator for (Int, K) pairs that orders them both by their partition ID and a key ordering
     * */
    private static class PartitionAndKeyComparator<K> implements Comparator<Tuple2<Integer, K>> {

        final Comparator<K> keyComparator;

        PartitionAndKeyComparator(Comparator<K> keyComparator) {
            this.keyComparator = keyComparator;
        }

        @Override
        public int compare(Tuple2<Integer, K> a, Tuple2<Integer, K> b) {
            int partitionDiff = a._1() - b._1();
            if (partitionDiff != 0) {
                return partitionDiff;
            }
            return keyComparator.compare(a._2(), b._2());
        }

    }


}
