package com.sdu.spark.scheduler;

import com.sdu.spark.Partition;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.TaskContext;
import com.sdu.spark.broadcast.Broadcast;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.utils.scala.Tuple2;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;

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
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long deserializeStartTime = System.currentTimeMillis();
        long deserializeStartCpuTime = threadMXBean.isCurrentThreadCpuTimeSupported() ? threadMXBean.getCurrentThreadCpuTime()
                                                                                      : 0L;
        SerializerInstance serializer = SparkEnv.env.closureSerializer.newInstance();

        Tuple2<RDD<T>, BroadCastFunc<Iterator<T>, U>> res = serializer.deserialize(ByteBuffer.wrap(taskBinary.value()),
                                                                                           Thread.currentThread().getContextClassLoader());
        executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime;
        executorDeserializeCpuTime = threadMXBean.isCurrentThreadCpuTimeSupported() ? threadMXBean.getCurrentThreadCpuTime() - deserializeStartCpuTime
                                                                                    : 0L;

        assert res != null;
        BroadCastFunc<Iterator<T>, U> func = res._2();
        RDD<T> rdd = res._1();
        Iterator<T> iterator = rdd.iterator(partition, context);
        return func.broadcast(context, iterator);
    }

    @Override
    public TaskLocation[] preferredLocations() {
        return preferredLocs;
    }

    @Override
    public String toString() {
        return String.format("ResultTask(%d, %d)", stageId, partitionId);
    }

    public interface BroadCastFunc<T, U> {
        U broadcast(TaskContext context, T iterator);
    }
}
