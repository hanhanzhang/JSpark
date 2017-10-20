package com.sdu.spark.shuffle.sort;

/**
 * @author hanhan.zhang
 * */
public class PackedRecordPointer {

    /**
     * The maximum partition identifier that can be encoded. Note that partition ids start from 0.
     * */
    static final int MAXIMUM_PARTITION_ID = (1 << 24) - 1;  // 16777215

}
