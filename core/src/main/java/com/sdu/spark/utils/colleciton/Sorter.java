package com.sdu.spark.utils.colleciton;

import java.util.Comparator;

/**
 * @author hanhan.zhang
 * */
public class Sorter<K, Buffer> {

    private final TimeSort<K, Buffer> timeSort;

    public Sorter(SortDataFormat<K, Buffer> s) {
        this.timeSort = new TimeSort<>(s);
    }

    /**
     * Sorts the input buffer within range [lo, hi).
     */
    public void sort(Buffer a, int lo, int hi, Comparator<K> c) {
        timeSort.sort(a, lo, hi, c);
    }
}
