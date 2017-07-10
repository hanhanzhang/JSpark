package com.sdu.spark.executor;

import com.sdu.spark.scheduler.TaskState;

import java.nio.ByteBuffer;

/**
 * Executor通过{@link ExecutorBackend}向Cluster上报任务运行状态
 *
 * @author hanhan.zhang
 * */
public interface ExecutorBackend {

    void statusUpdate(long taskId, TaskState state, ByteBuffer data);


}
