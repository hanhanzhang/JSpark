package com.sdu.spark.scheduler;

/**
 * @author hanhan.zhang
 * */
public interface JobListener {

    void taskSucceeded(int index, Object result);
    void jobFailed(Exception exception);

}
