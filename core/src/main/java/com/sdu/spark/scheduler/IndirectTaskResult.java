package com.sdu.spark.scheduler;

import com.sdu.spark.storage.BlockId;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class IndirectTaskResult<T> extends DirectTaskResult<T> {

    public BlockId blockId;
    public int size;

    public IndirectTaskResult(ByteBuffer valueBytes, BlockId blockId, int size) {
        super(valueBytes);
        this.blockId = blockId;
        this.size = size;
    }
}
