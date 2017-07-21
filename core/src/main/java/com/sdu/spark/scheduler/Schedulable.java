package com.sdu.spark.scheduler;

/**
 * @author hanhan.zhang
 * */
public interface Schedulable {

    void addSchedulable(Schedulable schedulable);

}
