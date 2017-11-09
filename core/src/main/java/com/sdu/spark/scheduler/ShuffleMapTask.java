package com.sdu.spark.scheduler;

import com.sdu.spark.*;
import com.sdu.spark.broadcast.Broadcast;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.shuffle.ShuffleManager;
import com.sdu.spark.shuffle.ShuffleWriter;
import com.sdu.spark.utils.scala.Product2;
import com.sdu.spark.utils.scala.Tuple2;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;

import static java.lang.Thread.currentThread;
import static java.nio.ByteBuffer.wrap;

/**
 * @author hanhan.zhang
 * */
public class ShuffleMapTask extends Task<MapStatus> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleMapTask.class);

    private transient TaskLocation[] preferredLocs;
    private Broadcast<byte[]> taskBinary;
    private Partition partition;

    public ShuffleMapTask(int partitionId) {
        this(0, 0, null, new Partition() {
            @Override
            public int index() {
                return 0;
            }
        }, null, new Properties(), 0, null, null);
    }

    public ShuffleMapTask(int stageId,
                          int stageAttemptId,
                          Broadcast<byte[]> taskBinary,
                          Partition partition,
                          TaskLocation[] locs,
                          Properties localProperties,
                          int jobId,
                          String appId,
                          String appAttemptId) {
        super(stageId, stageAttemptId, partition.index(), localProperties, jobId, appId, appAttemptId);
        this.preferredLocs = locs;
        this.taskBinary = taskBinary;
        this.partition = partition;
    }

    @SuppressWarnings("unchecked")
    @Override
    public MapStatus runTask(TaskContext context) throws Exception {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long deserializeStartTime = System.currentTimeMillis();
        long deserializeStartCpuTime = bean.isCurrentThreadCpuTimeSupported() ? bean.getCurrentThreadCpuTime()
                                                                              : 0L;
        SerializerInstance ser = SparkEnv.env.closureSerializer.newInstance();
        // DAGScheduler.submitMissingTasks
        // ShuffleMapStage ==> Tuple2<RDD, ShuffleDependency>
        Tuple2<RDD<?>, ShuffleDependency<?, ?, ?>> res = ser.deserialize(wrap(taskBinary.value()),
                                                                              currentThread().getContextClassLoader());

        executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime;
        executorDeserializeCpuTime = bean.isCurrentThreadCpuTimeSupported() ? bean.getCurrentThreadCpuTime() - deserializeStartCpuTime
                                                                                    : 0L;

        assert res != null;
        RDD<?> rdd = res._1();
        ShuffleDependency<?, ?, ?> dep = res._2();
        ShuffleWriter<Object, Object> writer = null;
        try {
            ShuffleManager manager = SparkEnv.env.shuffleManager;
            writer = manager.getWriter(dep.shuffleHandle(), partitionId, context);
            Iterator<Product2<Object, Object>> iterator = (Iterator<Product2<Object, Object>>) rdd.iterator(partition, context);
            writer.write(iterator);
            return writer.stop(true);
        } catch (Exception e){
            try {
                if (writer != null) {
                    writer.stop(false);
                }
            } catch (Exception ex) {
                LOGGER.debug("Could not stop writer", e);
            }
            throw new SparkException(e);
        }
    }

    @Override
    public TaskLocation[] preferredLocations() {
        return preferredLocs;
    }

    @Override
    public String toString() {
        return String.format("ShuffleMapTask(%d, %d)", stageId, partitionId);
    }
}
