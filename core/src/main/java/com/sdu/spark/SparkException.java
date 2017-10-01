package com.sdu.spark;

/**
 * @author hanhan.zhang
 * */
public class SparkException extends RuntimeException {

    public SparkException(String message) {
        super(message);
    }

    public SparkException(Throwable cause) {
        super(cause);
    }

    public SparkException(String message, Throwable cause) {
        super(message, cause);
    }
}
