package com.sdu.spark.scheduler.action;

import com.sdu.spark.TaskContext;
import com.sdu.spark.utils.TIterator;

import java.io.Serializable;

/**
 * RDD Job触发算子
 *
 * @author hanhan.zhang
 * */
public interface JobAction<T, U> extends Serializable {

    U func(TaskContext context, TIterator<T> dataIterator);

}
