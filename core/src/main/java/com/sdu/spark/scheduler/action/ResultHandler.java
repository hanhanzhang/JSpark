package com.sdu.spark.scheduler.action;

/**
 * @author hanhan.zhang
 * */
public interface ResultHandler<U> {

    void callback(int index, U result);

}
