package com.sdu.spark.scheduler.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.ExecutorAllocationClient;
import com.sdu.spark.rpc.*;
import com.sdu.spark.scheduler.*;
import com.sdu.spark.scheduler.cluster.CoarseGrainedClusterMessage.*;
import com.sdu.spark.utils.SerializableBuffer;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.sdu.spark.utils.RpcUtils.maxMessageSizeBytes;
import static com.sdu.spark.utils.ThreadUtils.newDaemonSingleThreadScheduledExecutor;

/**
 * @author hanhan.zhang
 * */
public class CoarseGrainedSchedulerBackend implements ExecutorAllocationClient, SchedulerBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoarseGrainedSchedulerBackend.class);

    private SparkConf conf;
    private RpcEnv rpcEnv;
    private int maxRpcMessageSize;


    /*********************************Spark Job分配Executor资源******************************/
    private TaskSchedulerImpl scheduler;
    private Map<String, ExecutorData> executorDataMap = Maps.newHashMap();
    private Map<String, Boolean> executorsPendingToRemove = Maps.newHashMap();

    public CoarseGrainedSchedulerBackend(TaskSchedulerImpl scheduler) {
        this.scheduler = scheduler;
        this.conf = this.scheduler.sc.conf;
        this.rpcEnv = this.scheduler.sc.env.rpcEnv;
        this.maxRpcMessageSize = maxMessageSizeBytes(conf);
    }

    class DrvierEndPonit extends RpcEndPoint {
        protected Set<String> executorsPendingLossReason = Sets.newHashSet();
        protected Map<RpcAddress, String> addressToExecutorId = Maps.newHashMap();
        private ScheduledExecutorService reviveThread = newDaemonSingleThreadScheduledExecutor("driver-receive-thread");

        @Override
        public RpcEndPointRef self() {
            return rpcEnv.endPointRef(this);
        }

        @Override
        public void onStart() {
            long reviveIntervalMs = conf.getTimeAsMs("spark.scheduler.revive.interval", "1s");
            reviveThread.scheduleAtFixedRate(() -> self().send(new ReviveOffers()),
                                0, reviveIntervalMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void receive(Object msg) {
            if (msg instanceof StatusUpdate) {
                StatusUpdate statusUpdate = (StatusUpdate) msg;
                scheduler.statusUpdate(statusUpdate.taskId, statusUpdate.state, statusUpdate.data.buffer);
                if (TaskState.isFinished(statusUpdate.state)) {
                    ExecutorData executorData = executorDataMap.get(statusUpdate.executorId);
                    if (executorData != null) {
                        LOGGER.info("Executor(execId = {})运行完Spark Task(taskId = {}, taskState = {}), 重新分发Spark Task",
                                statusUpdate.executorId, statusUpdate.taskId, statusUpdate.state);
                        executorData.freeCores += scheduler.CPUS_PER_TASK;
                        makeOffers(statusUpdate.executorId);
                    } else {
                        LOGGER.info("收到未知Executor(execId = {}) Spark Task状态变更: taskId = {}, state = {}",
                                statusUpdate.executorId, statusUpdate.taskId, statusUpdate.state);
                    }
                }
            } else if (msg instanceof ReviveOffers){

            }
        }

        @Override
        public void receiveAndReply(Object msg, RpcCallContext context) {

        }

        private void makeOffers(String executorId) {
            List<TaskDescription> taskDescs = null;
            synchronized (CoarseGrainedSchedulerBackend.this) {
                if (executorIsAlive(executorId)) {
                    ExecutorData executorData = executorDataMap.get(executorId);
                    List<WorkerOffer> workerOffers = Lists.newArrayList(
                            new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores)
                    );
                    taskDescs = scheduler.resourceOffers(workerOffers);
                } else {
                    taskDescs = Collections.emptyList();
                }
            }

            if (CollectionUtils.isNotEmpty(taskDescs)) {
                launchTask(taskDescs);
            }
        }

        private void launchTask(List<TaskDescription> tasks) {
            for (TaskDescription task : tasks) {
                try {
                    ByteBuffer buffer = TaskDescription.encode(task);
                    if (buffer.limit() >= maxRpcMessageSize) {
                        String msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                                "spark.rpc.message.maxSize (%d bytes). Consider increasing " +
                                "spark.rpc.message.maxSize or using broadcast variables for large values.";
                        msg = String.format(msg, task.taskId, task.index, buffer.limit(), maxRpcMessageSize);
                        TaskSetManager tasSetMgr = scheduler.taskIdToTaskSetManager.get(task.taskId);
                        tasSetMgr.abort(msg);
                    } else {
                        ExecutorData executorData = executorDataMap.get(task.executorId);
                        executorData.freeCores -= scheduler.CPUS_PER_TASK;
                        LOGGER.debug("Executor(execId = {}, execAddress = {})分配Spark Task(taskId = {}, taskName = {})",
                                task.executorId, executorData.executorHost, task.taskId, task.name);
                        executorData.executorEndpoint.send(new LaunchTask(new SerializableBuffer(buffer)));
                    }
                } catch (IOException e) {
                    LOGGER.error("Task(taskId = {})序列化异常", task.taskId, e);
                }

            }
        }

        private boolean executorIsAlive(String executorId) {
            synchronized (CoarseGrainedClusterMessage.class) {
                return !executorsPendingToRemove.containsKey(executorId) &&
                        !executorsPendingLossReason.contains(executorId);
            }
        }
    }

    @Override
    public void start() {


    }

    @Override
    public List<String> getExecutorIds() {
        return null;
    }

    @Override
    public boolean requestTotalExecutors(int numExecutors, int localityAwareTasks, Map<String, Integer> hostToLocalTaskCount) {
        return false;
    }

    @Override
    public boolean requestExecutors(int numAdditionalExecutors) {
        return false;
    }

    @Override
    public List<String> killExecutors(List<String> executorIds, boolean replace, boolean force) {
        return null;
    }

    @Override
    public boolean killExecutorsOnHost(String host) {
        return false;
    }
}
