package com.sdu.spark.storage;

/**
 * @author hanhan.zhang
 * */
public class BlockNotFoundException extends RuntimeException {

    public BlockNotFoundException(String blockId) {
        super(String.format("Block %s not found", blockId));
    }
}
