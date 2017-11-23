package com.sdu.spark.scheduler.action;

import com.sdu.spark.TaskContext;

import java.io.Serializable;
import java.util.Iterator;

/**
 * RDD Job触发算子
 *
 * @author hanhan.zhang
 * */
public interface JobAction<T, U> extends Serializable{

    U func(TaskContext context, Iterator<T> dataIterator);

}
