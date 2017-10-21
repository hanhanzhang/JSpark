package com.sdu.spark.storage;

/**
 * @author hanhan.zhang
 * */
public class BlockException extends RuntimeException {

    private BlockId blockId;

    private String message;

    public BlockException(String message, BlockId blockId) {
        super(message);
        this.blockId = blockId;
    }
}
