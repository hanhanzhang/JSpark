package com.sdu.spark.scheduler.action;

/**
 * @author hanhan.zhang
 * */
public interface PartitionResultHandler<U> {

    void handle(int index, U result);

}
