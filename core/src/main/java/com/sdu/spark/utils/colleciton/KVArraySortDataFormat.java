package com.sdu.spark.utils.colleciton;

import com.sdu.spark.utils.scala.Tuple2;

/**
 * Supports sorting an array of key-value pairs where the elements of the array alternate between
 * keys and values, as used in {@link AppendOnlyMap}.
 *
 * K Type of the sort key of each element
 *
 * T Type of the Array we're sorting. Typically this must extend AnyRef, to support cases
 *           when the keys and values are not the same type.
 *
 * @author hanhan.zhang
 * */
@SuppressWarnings("unchecked")
public class KVArraySortDataFormat<K, T> extends SortDataFormat<K, T[]>{

    @Override
    public K getKey(T[] data, int pos) {
        return (K) data[2 * pos];
    }

    @Override
    public void swap(T[] data, int pos0, int pos1) {
        T tmpKey = data[2 * pos0];
        T tmpValue = data[2 * pos0 + 1];
        data[2 * pos0] = data[2 * pos1];
        data[2 * pos0 + 1] = data[2 * pos1 + 1];
        data[2 * pos1] = tmpKey;
        data[2 * pos1 + 1] = tmpValue;
    }

    @Override
    public void copyElement(T[] src, int srcPos, T[] dst, int dstPos) {
        dst[2 * dstPos] = src[2 * srcPos];
        dst[2 * dstPos + 1] = src[2 * srcPos + 1];
    }

    @Override
    public void copyRange(T[] src, int srcPos, T[] dst, int dstPos, int length) {
        System.arraycopy(src, srcPos, dst, dstPos, length);
    }

    @Override
    public T[] allocate(int length) {
        return (T[]) new Object[length * 2];
    }

    public static void main(String[] args) {
        Object[] buffer = new Object[]{new Tuple2<>(1, "A"), 63, new Tuple2<>(1, "B"), 56, new Tuple2<>(2, "C"), 58};
        KVArraySortDataFormat<Tuple2<Integer, String>, Object> dataFormat = new KVArraySortDataFormat<>();
        Tuple2<Integer, String> key = dataFormat.getKey(buffer, 0);
        System.out.println("Partition: " + key._1() + ", Key: " + key._2());
    }

}
