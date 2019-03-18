package com.sdu.spark.scheduler;

import com.sdu.spark.*;
import com.sdu.spark.broadcast.Broadcast;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.shuffle.ShuffleManager;
import com.sdu.spark.shuffle.ShuffleWriter;
import com.sdu.spark.utils.TIterator;
import com.sdu.spark.utils.scala.Product2;
import com.sdu.spark.utils.scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Properties;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.nio.ByteBuffer.wrap;

/**
 *                                 +------------------+
 *                                 | MapOutputTracker |
 *                                 +------------------+
 *                                    /|\       /|\
 *                                     |         |
 *  +--------------------------+       |         |        +--------------------------+
 *  |         Stage            |-------+         +--------|         Stage            |
 *  | +----------+  +--------+ |                          |  +------+   +----------+ |
 *  | | pipeline |  |  write | |<------------------------>|  | read |   | pipeline | |
 *  | +----------+  +--------+ |                          |  +------+   +----------+ |
 *  +--------------------------+                          +--------------------------+
 *
 *  ShuffleMapTask 进行 shuffle write, 数据存储 BlockManager并将数据位置信息上报 Driver 的 MapOutputTracker
 *
 *  ShuffleMapTask 运行结果 MapStatus, 记录 partition 的 offset
 *
 *  ShuffleMapTask Spill 过程中由 TaskMemoryManager 申请计算内存(Execution Memory)
 *
 * @author hanhan.zhang
 * */
public class ShuffleMapTask extends Task<MapStatus> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleMapTask.class);

    private transient TaskLocation[] preferredLocations;
    private Broadcast<byte[]> taskBinary;
    private Partition partition;

    public ShuffleMapTask(int stageId, int stageAttemptId, Broadcast<byte[]> taskBinary, Partition partition, TaskLocation[] locations,
                          Properties localProperties, int jobId, String appId, String appAttemptId) {
        super(stageId, stageAttemptId, partition.index(), localProperties, jobId, appId, appAttemptId);
        this.preferredLocations = locations;
        this.taskBinary = taskBinary;
        this.partition = partition;
    }

    @SuppressWarnings("unchecked")
    @Override
    public MapStatus runTask(TaskContext context) throws Exception {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long deserializeStartTime = System.currentTimeMillis();
        long deserializeStartCpuTime = 0L;
        if (bean.isCurrentThreadCpuTimeSupported()) {
            deserializeStartCpuTime = bean.getCurrentThreadCpuTime();
        }
        SerializerInstance ser = SparkEnv.env.closureSerializer.newInstance();

        // DAGScheduler.submitMissingTasks
        // ShuffleMapStage ==> Tuple2<RDD, ShuffleDependency>
        Tuple2<RDD<?>, ShuffleDependency<?, ?, ?>> res = ser.deserialize(wrap(taskBinary.value()), currentThread().getContextClassLoader());

        executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime;
        executorDeserializeCpuTime = 0L;
        if (bean.isCurrentThreadCpuTimeSupported()) {
            executorDeserializeCpuTime = bean.getCurrentThreadCpuTime() - deserializeStartCpuTime;
        }
        assert res != null;
        RDD<?> rdd = res._1();
        ShuffleDependency<?, ?, ?> dep = res._2();
        ShuffleWriter<Object, Object> writer = null;
        try {
            ShuffleManager manager = SparkEnv.env.shuffleManager;
            writer = manager.getWriter(dep.shuffleHandle(), partitionId, context);
            TIterator<Product2<Object, Object>> iterator = (TIterator<Product2<Object, Object>>) rdd.iterator(partition, context);
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
        return preferredLocations;
    }

    @Override
    public String toString() {
        return format("ShuffleMapTask(%d, %d)", stageId, partitionId);
    }
}
