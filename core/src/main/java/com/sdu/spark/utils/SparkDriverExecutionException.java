package com.sdu.spark.utils;

import com.sdu.spark.SparkException;

public class SparkDriverExecutionException extends SparkException {

    public SparkDriverExecutionException(Throwable cause) {
        super("Execution error", cause);
    }
}
