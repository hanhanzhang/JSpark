package com.sdu.spark.shuffle;

import com.sdu.spark.storage.BlockManagerId;

/**
 * @author hanhan.zhang
 * */
public class FetchFailedException extends RuntimeException {

    public BlockManagerId bmAddress;
    public int shuffleId;
    public int mapId;
    public int reduceId;
    public String message;
    public Throwable cause;

    public FetchFailedException(BlockManagerId bmAddress, int shuffleId, int mapId, int reduceId,
                                Throwable cause) {
        this(bmAddress, shuffleId, mapId, reduceId, cause.getMessage(), cause);
    }

    public FetchFailedException(BlockManagerId bmAddress, int shuffleId, int mapId, int reduceId,
                                String message, Throwable cause) {
        this.bmAddress = bmAddress;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.reduceId = reduceId;
        this.message = message;
        this.cause = cause;
    }

    public static class MetadataFetchFailedException extends FetchFailedException {

        public MetadataFetchFailedException(int shuffleId, int reduceId, String message) {
            super(null, shuffleId, -1, reduceId, message, null);
        }
    }
}
