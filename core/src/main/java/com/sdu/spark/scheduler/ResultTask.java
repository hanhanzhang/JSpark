package com.sdu.spark.scheduler;

import com.sdu.spark.Partition;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.TaskContext;
import com.sdu.spark.broadcast.Broadcast;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.scheduler.action.RDDAction;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.utils.scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Iterator;
import java.util.Properties;

import static java.lang.Thread.currentThread;
import static java.nio.ByteBuffer.wrap;

/**
 * @author hanhan.zhang
 * */
public class ResultTask<T, U> extends Task<U> implements Serializable {

    private transient TaskLocation[] preferredLocs;
    private Broadcast<byte[]> taskBinary;
    private Partition partition;

    public ResultTask(int stageId,
                      int stageAttemptId,
                      Broadcast<byte[]> taskBinary,
                      Partition partition,
                      TaskLocation[] locs,
                      int outputId,
                      Properties localProperties,
                      int jobId,
                      String appId,
                      String appAttemptId) {
        super(stageId, stageAttemptId, partition.index(), localProperties, jobId, appId, appAttemptId);
        this.preferredLocs = locs;
        this.taskBinary = taskBinary;
        this.partition = partition;
    }

    @Override
    public U runTask(TaskContext context) throws IOException {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long deserializeStartTime = System.currentTimeMillis();
        long deserializeStartCpuTime = bean.isCurrentThreadCpuTimeSupported() ? bean.getCurrentThreadCpuTime()
                                                                                      : 0L;
        SerializerInstance serializer = SparkEnv.env.closureSerializer.newInstance();

        // DAGScheduler.submitMissingTasks()
        // ResultStage ===> Tuple2<RDD, RDDAction<T, U>>
        // RDDAction即触发Spark Job动作因子
        Tuple2<RDD<T>, RDDAction<T, U>> res = serializer.deserialize(wrap(taskBinary.value()),
                                                                                   currentThread().getContextClassLoader());
        executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime;
        executorDeserializeCpuTime = bean.isCurrentThreadCpuTimeSupported() ? bean.getCurrentThreadCpuTime() - deserializeStartCpuTime
                                                                                    : 0L;

        assert res != null;
        RDDAction<T, U> action = res._2();
        RDD<T> rdd = res._1();
        Iterator<T> iterator = rdd.iterator(partition, context);
        return action.func(context, iterator);
    }

    @Override
    public TaskLocation[] preferredLocations() {
        return preferredLocs;
    }

    @Override
    public String toString() {
        return String.format("ResultTask(%d, %d)", stageId, partitionId);
    }
}
