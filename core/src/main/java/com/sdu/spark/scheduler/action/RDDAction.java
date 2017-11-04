package com.sdu.spark.scheduler.action;

import com.sdu.spark.TaskContext;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public interface RDDAction<T, U> {

    U func(TaskContext context, Iterator<T> dataIterator);

}
