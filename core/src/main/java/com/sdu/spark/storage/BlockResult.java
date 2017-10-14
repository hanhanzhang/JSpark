package com.sdu.spark.storage;

import com.sdu.spark.executor.DataReadMethod;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public class BlockResult {
    public Iterator<?> data;
    public DataReadMethod readMethod;
    public long bytes;

    public BlockResult(Iterator<?> data,
                       DataReadMethod readMethod,
                       long bytes) {
        this.data = data;
        this.readMethod = readMethod;
        this.bytes = bytes;
    }
}
