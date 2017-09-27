package com.sdu.spark;

/**
 * @author hanhan.zhang
 * */
public class SparkException extends Exception {

    public SparkException(String message) {
        super(message);
    }

    public SparkException(String message, Throwable cause) {
        super(message, cause);
    }
}
