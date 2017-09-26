package com.sdu.spark.network.client;

/**
 * @author hanhan.zhang
 * */
public class ChunkFetchFailureException extends RuntimeException {

    public ChunkFetchFailureException(String errorMsg) {
        super(errorMsg);
    }

}
