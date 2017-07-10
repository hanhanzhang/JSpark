package com.sdu.spark.utils;

import com.sdu.spark.TaskContext;

import java.util.EventListener;

/**
 * @author hanhan.zhang
 * */
public interface TaskCompletionListener extends EventListener {

    void onTaskCompletion(TaskContext context);

}
