package com.sdu.spark.scheduler;

import com.sdu.spark.SparkEnv;
import com.sdu.spark.scheduler.TaskEndReason.TaskResultLost;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.utils.ChunkedByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import static com.sdu.spark.utils.ThreadUtils.newDaemonFixedThreadPool;

/**
 * @author hanhan.zhang
 * */
public class TaskResultGetter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskResultGetter.class);

    private SparkEnv sparkEnv;
    private TaskSchedulerImpl scheduler;

    private int THREADS;
    private ExecutorService getTaskResultExecutor;

    private ThreadLocal<SerializerInstance> serializer = new ThreadLocal<SerializerInstance>(){
        @Override
        protected SerializerInstance initialValue() {
            return sparkEnv.closureSerializer.newInstance();
        }
    };

    private ThreadLocal<SerializerInstance> taskResultSerializer = new ThreadLocal<SerializerInstance>() {
        @Override
        protected SerializerInstance initialValue() {
            return sparkEnv.serializer.newInstance();
        }
    };

    public TaskResultGetter(SparkEnv sparkEnv,
                            TaskSchedulerImpl scheduler) {
        this.sparkEnv = sparkEnv;
        this.scheduler = scheduler;

        this.THREADS = this.sparkEnv.conf.getInt("spark.resultGetter.threads", 4);
        this.getTaskResultExecutor = newDaemonFixedThreadPool(THREADS, "task-result-getter");
    }

    public void enqueueSuccessfulTask(TaskSetManager taskSetManager,
                                      long tid,
                                      ByteBuffer serializedData) {
        getTaskResultExecutor.execute(() -> {
            try {
                TaskResult<?> taskResult = serializer.get().deserialize(serializedData);
                DirectTaskResult<?> result = null;
                if (taskResult instanceof DirectTaskResult) {
                    if (taskResult instanceof IndirectTaskResult) {
                        IndirectTaskResult<?> indirectTaskResult = (IndirectTaskResult<?>) taskResult;
                        if (!taskSetManager.canFetchMoreResults(indirectTaskResult.size)) {
                            // dropped by executor if size is larger than maxResultSize
                            sparkEnv.blockManager.master.removeBlock(indirectTaskResult.blockId);
                            return;
                        }

                        LOGGER.debug("Fetching indirect task result for TID {}", tid);
                        scheduler.handleTaskGettingResult(taskSetManager, tid);
                        ChunkedByteBuffer serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(indirectTaskResult.blockId);
                        if (serializedTaskResult == null) {
                            scheduler.handleFailedTask(taskSetManager,
                                                       tid,
                                                       TaskState.FINISHED,
                                                       new TaskResultLost());
                            return;
                        }
                        DirectTaskResult<?> deserializedResult=  serializer.get().deserialize(serializedTaskResult.toByteBuffer());
                        // force deserialization of referenced value
                        deserializedResult.value(taskResultSerializer.get());
                        sparkEnv.blockManager.master.removeBlock(indirectTaskResult.blockId);

                        result = indirectTaskResult;
                    } else {
                        result = (DirectTaskResult<?>) taskResult;
                        if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
                            return;
                        }
                        // deserialize "value" without holding any lock so that it won't block other threads.
                        // We should call it here, so that when it's called again in
                        // "TaskSetManager.handleSuccessfulTask", it does not need to deserialize the value.
                        result.value(taskResultSerializer.get());
                    }

                    // TODO: AccumulatorV2
                    scheduler.handleSuccessfulTask(taskSetManager, tid, result);
                }
            }  catch (Exception e) {

            }

        });
    }

    public void enqueueFailedTask(TaskSetManager taskSetManager,
                                  long tid,
                                  TaskState taskState,
                                  ByteBuffer serializedData) {

    }
}
