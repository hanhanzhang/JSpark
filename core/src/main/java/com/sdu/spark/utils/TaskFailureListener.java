package com.sdu.spark.utils;

import com.sdu.spark.TaskContext;

import java.util.EventListener;

/**
 * @author hanhan.zhang
 * */
public interface TaskFailureListener extends EventListener {

    void onTaskFailure(TaskContext context, Throwable error);

}
