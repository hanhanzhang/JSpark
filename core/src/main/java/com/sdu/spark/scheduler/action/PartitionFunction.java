package com.sdu.spark.scheduler.action;

import com.sdu.spark.TaskContext;
import com.sdu.spark.utils.TIterator;

import java.io.Serializable;

/**
 * RDD分区计算函数
 *
 * @author hanhan.zhang
 * */
public interface PartitionFunction<T, U> extends Serializable {

    U func(TaskContext context, TIterator<T> dataIterator);

}
