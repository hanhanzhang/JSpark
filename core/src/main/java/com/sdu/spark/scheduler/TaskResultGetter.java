package com.sdu.spark.scheduler;

import com.sdu.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class TaskResultGetter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskResultGetter.class);

    private SparkEnv sparkEnv;
    private TaskSchedulerImpl scheduler;

    public TaskResultGetter(SparkEnv sparkEnv,
                            TaskSchedulerImpl scheduler) {
        this.sparkEnv = sparkEnv;
        this.scheduler = scheduler;
    }

    public void enqueueSuccessfulTask(TaskSetManager taskSet,
                                      long tid,
                                      ByteBuffer serializedData) {

    }

    public void enqueueFailedTask(TaskSetManager taskSetManager,
                                  long tid,
                                  TaskState taskState,
                                  ByteBuffer serializedData) {

    }
}
