package com.sdu.spark.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author hanhan.zhang
 * */
public class Utils {

    public static <T> T getFutureResult(Future<?> future) {
        try {
            return (T) future.get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            return null;
        }
    }

}
