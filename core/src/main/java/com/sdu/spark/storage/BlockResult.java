package com.sdu.spark.storage;

import com.sdu.spark.executor.DataReadMethod;
import com.sdu.spark.utils.TIterator;

/**
 * BlockResult封装从本地BlockManager中获取Block数据及与Block相关的度量数据
 *
 * @author hanhan.zhang
 * */
public class BlockResult {
    private TIterator<?> data;
    private DataReadMethod readMethod;
    private long bytes;

    public BlockResult(TIterator<?> data,
                       DataReadMethod readMethod,
                       long bytes) {
        this.data = data;
        this.readMethod = readMethod;
        this.bytes = bytes;
    }

    public TIterator<?> getData() {
        return data;
    }

    public DataReadMethod getReadMethod() {
        return readMethod;
    }

    public long getBytes() {
        return bytes;
    }
}
