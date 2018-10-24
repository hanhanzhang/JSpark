package com.sdu.spark.scheduler.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.ExecutorAllocationClient;
import com.sdu.spark.SparkException;
import com.sdu.spark.executor.ExecutorExitCode.ExecutorLossReason;
import com.sdu.spark.executor.ExecutorExitCode.SlaveLost;
import com.sdu.spark.rpc.*;
import com.sdu.spark.scheduler.*;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerExecutorAdded;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerExecutorRemoved;
import com.sdu.spark.scheduler.cluster.CoarseGrainedClusterMessage.*;
import com.sdu.spark.utils.SerializableBuffer;
import com.sdu.spark.utils.scala.Tuple2;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.sdu.spark.utils.RpcUtils.maxMessageSizeBytes;
import static com.sdu.spark.utils.ThreadUtils.newDaemonSingleThreadScheduledExecutor;
import static com.sdu.spark.utils.Utils.getFutureResult;
import static com.sdu.spark.utils.Utils.partition;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * {@link CoarseGrainedSchedulerBackend}职责:
 *
 * 1: CoarseGrainedSchedulerBackend在SparkContext中被初始化(也就是说, 只有在Driver端会初始化该组件), 其初始化调用链:
 *
 *    SparkContext.createTaskScheduler()[Driver]
 *      |
 *      +---> CoarseGrainedSchedulerBackend::new
 *
 * 2: CoarseGrainedSchedulerBackend.start(): Driver注册DriverEndpoint节点, DriverEndpoint处理Rpc消息(关键):
 *
 *   1': StatusUpdate[Executor ShuffleMapTask/ResultTask计算结果], 其调用链:
 *
 *      CoarseGrainedExecutorBackend.statusUpdate(StatusUpdate)[Executor]
 *        |
 *        +--> CoarseGrainedSchedulerBackend.DriverEndpoint.receive(StatusUpdate)[Driver]
 *               |
 *               +--> TaskScheduler.statusUpdate(StatusUpdate)[Driver]
 *                      |
 *                      +--> TaskResultGetter.enqueueSuccessfulTask()/enqueueFailedTask()[Driver]
 *                      |
 *                      +--> DAGScheduler.handleExecutorLost()[Task run Failure, Driver]
 *                             |
 *                             +--> MapOutputTrackerMaster.removeOutputOnHost()/removeOutputOnExecutor[Driver]
 *
 *
 *   2': RegisterExecutor[Executor注册], 调用链:
 *
 *      CoarseGrainedExecutorBackend.onStart()[Executor]
 *        |
 *        +--> CoarseGrainedExecutorBackend.receive(RegisteredExecutor)[Executor]
 *               |
 *               +--> CoarseGrainedSchedulerBackend.DriverEndpoint.receiveAndReply(RegisteredExecutor)[Driver]
 *
 * 3: Executor资源管理
 *
 *  1': 向Spark Master申请Executor资源
 *
 *  2': 请求Spark Master关闭Executor
 *
 * @author hanhan.zhang
 * */
public abstract class CoarseGrainedSchedulerBackend extends SchedulerBackend implements ExecutorAllocationClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoarseGrainedSchedulerBackend.class);

    public static String ENDPOINT_NAME = "CoarseGrainedScheduler";

    protected SparkConf conf;
    private RpcEnv rpcEnv;
    private LiveListenerBus listenerBus;
    private int maxRpcMessageSize;
    private long createTime = System.currentTimeMillis();
    private long maxRegisteredWaitingTimeMs;


    /*******************************Spark Job分配Executor资源管理******************************/
    protected TaskSchedulerImpl scheduler;
    // Spark Job分配CPU核数
    protected AtomicInteger totalCoreCount = new AtomicInteger(0);
    protected double minRegisteredRatio;

    // Spark Job分配Executor
    @GuardedBy("CoarseGrainedSchedulerBackend.this")
    private int requestedTotalExecutors = 0;

    // Spark Job等待分配Executor
    @GuardedBy("CoarseGrainedSchedulerBackend.this")
    private int numPendingExecutors = 0;

    // Spark Job注册Executor数
    private AtomicInteger totalRegisteredExecutors = new AtomicInteger(0);
    private int localityAwareTasks = 0;
    @GuardedBy("CoarseGrainedSchedulerBackend.this")
    private Map<String, Integer> hostToLocalTaskCount = Collections.emptyMap();
    // Spark Job分配Executor[key = executorId, value = ExecutorData]
    private Map<String, ExecutorData> executorDataMap = Maps.newHashMap();

    @GuardedBy("CoarseGrainedSchedulerBackend.this")
    private Map<String, Boolean> executorsPendingToRemove = Maps.newHashMap();

    /*********************************Spark Job Driver***********************************/
    private RpcEndpointRef driverEndpoint;

    public CoarseGrainedSchedulerBackend(TaskSchedulerImpl scheduler) {
        this.scheduler = scheduler;
        this.conf = this.scheduler.sc.conf;
        this.rpcEnv = this.scheduler.sc.env.rpcEnv;
        this.listenerBus = this.scheduler.sc.listenerBus;
        this.maxRpcMessageSize = maxMessageSizeBytes(conf);
        this.maxRegisteredWaitingTimeMs = conf.getTimeAsMs("spark.scheduler.maxRegisteredResourcesWaitingTime", "30");
        this.minRegisteredRatio = Math.min(1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0));
    }

    private RpcEndpoint createDriverEndPoint(Map<String, String> properties) {
        return new DriverEndpoint(this.rpcEnv, properties);
    }

    private class DriverEndpoint extends ThreadSafeRpcEndpoint {
        private Map<String, String> sparkProperties;

        protected Set<String> executorsPendingLossReason = Sets.newHashSet();
        // Executor分配地址映射[key = RpcAddress, value = executorId]
        private Map<RpcAddress, String> addressToExecutorId = Maps.newHashMap();
        // 定时调度Task分配给Executor线程
        private ScheduledExecutorService reviveThread = newDaemonSingleThreadScheduledExecutor("driver-receive-thread");

        public DriverEndpoint(RpcEnv rpcEnv, Map<String, String> sparkProperties) {
            super(rpcEnv);
            this.sparkProperties = sparkProperties;
        }

        @Override
        public void onStart() {
            long reviveIntervalMs = conf.getTimeAsMs("spark.scheduler.revive.interval", "1s");
            reviveThread.scheduleAtFixedRate(() -> self().send(new ReviveOffers()),
                                             0,
                                             reviveIntervalMs,
                                             TimeUnit.MILLISECONDS);
        }

        @Override
        public void receive(Object msg) {
            if (msg instanceof StatusUpdate) {
                StatusUpdate update = (StatusUpdate) msg;
                scheduler.statusUpdate(update.taskId, update.state, update.data.buffer);
                if (TaskState.isFinished(update.state)) {
                    ExecutorData executorData = executorDataMap.get(update.executorId);
                    if (executorData != null) {
                        executorData.freeCores += scheduler.CPUS_PER_TASK;
                        makeOffers(update.executorId);
                    } else {
                        LOGGER.warn("Ignored task status update ({} state {}) from unknown executor with ID {}",
                                    update.taskId, update.state, update.executorId);
                    }
                }
            } else if (msg instanceof ReviveOffers){
                makeOffers();
            } else if (msg instanceof KillTask) {
                KillTask killTask = (KillTask) msg;
                ExecutorData executorData = executorDataMap.get(killTask.executorId);
                if (executorData != null) {
                    executorData.executorEndpoint.send(killTask);
                }
            } else if (msg instanceof KillExecutorsOnHost) {
                KillExecutorsOnHost killExecutorsOnHost = (KillExecutorsOnHost) msg;
                Set<String> executors = scheduler.hostToExecutors.get(killExecutorsOnHost.host);
                if (CollectionUtils.isNotEmpty(executors)) {
                    killExecutors(Lists.newArrayList(executors), true, true);
                }
            }
        }

        @Override
        public void receiveAndReply(Object msg, RpcCallContext context) {
            if (msg instanceof RegisterExecutor) {
                registerExecutor((RegisterExecutor) msg, context);
            } else if (msg instanceof StopDriver) {
                context.reply(true);
                stop();
            } else if (msg instanceof StopExecutors) {
                executorDataMap.forEach((executorId, executorData) -> {
                    LOGGER.info("关闭Executor(execId = {}, address = {})", executorId, executorData.executorAddress.hostPort());
                    executorData.executorEndpoint.send(new StopExecutor());
                });
                context.reply(true);
            } else if (msg instanceof RemoveExecutor) {
                RemoveExecutor removeExecutor = (RemoveExecutor) msg;
                ExecutorData executorData = executorDataMap.get(removeExecutor.executorId);
                executorData.executorEndpoint.send(new StopExecutor());
                removeExecutor(removeExecutor.executorId, removeExecutor.reason);
                context.reply(true);
            } else if (msg instanceof RemoveWorker) {
                RemoveWorker removeWorker = (RemoveWorker) msg;
                removeWorker(removeWorker.workerId, removeWorker.host, removeWorker.message);
                context.reply(true);
            } else if (msg instanceof RetrieveSparkAppConfig) {

            }
        }

        @Override
        public void onDisconnected(RpcAddress remoteAddress) {
            String executorId = addressToExecutorId.get(remoteAddress);
            if (isNotEmpty(executorId)) {
                removeExecutor(executorId, new SlaveLost("Remote RPC client disassociated. Likely due to " +
                                                         "containers exceeding thresholds, or network issues. Check driver logs for WARN messages."));
            }
        }

        @Override
        public void onStop() {
            reviveThread.shutdownNow();
        }

        /********************************Spark Executor注册Driver*********************************/
        private void registerExecutor(RegisterExecutor executor, RpcCallContext context) {
            if (executorDataMap.containsKey(executor.executorId)) {
                executor.executorRef.send(new RegisterExecutorFailed("Duplicate executor ID: " + executor.executorId));
                context.reply(true);
            } else if (CollectionUtils.isNotEmpty(scheduler.nodeBlacklist()) &&
                        scheduler.nodeBlacklist().contains(executor.hostname)){
                executor.executorRef.send(new RegisterExecutorFailed(format("Executor is blacklisted: %s", executor.executorId)));
                context.reply(true);
            } else {
                RpcAddress executorAddress;
                if (executor.executorRef != null) {
                    executorAddress = executor.executorRef.address();
                } else {
                    executorAddress = context.senderAddress();
                }
                LOGGER.info("Driver注册Executor(address = {}, execId = {})", executorAddress, executor.executorId);
                addressToExecutorId.put(executorAddress, executor.executorId);
                totalCoreCount.addAndGet(executor.cores);
                totalRegisteredExecutors.addAndGet(1);
                ExecutorData executorData = new ExecutorData(executor.hostname, executor.cores, executor.executorRef,
                        executorAddress, executor.cores);
                synchronized (CoarseGrainedSchedulerBackend.this) {
                    executorDataMap.put(executor.executorId, executorData);
                }

                executor.executorRef.send(new RegisteredExecutor());
                // Note: some tests expect the reply to come after we put the executor in the map
                context.reply(true);
                listenerBus.post(
                        new SparkListenerExecutorAdded(System.currentTimeMillis(), executor.executorId, executorData));
                makeOffers();
            }
        }

        /********************************Spark Executor调度分配Task*********************************/
        private void makeOffers() {
            List<TaskDescription> taskDescriptions = null;
            synchronized (CoarseGrainedSchedulerBackend.this) {
                List<WorkerOffer> workerOffers = Lists.newLinkedList();
                executorDataMap.forEach((executorId, executorData) -> {
                    if (executorIsAlive(executorId)) {
                        workerOffers.add(new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores));
                    }
                });
                if (CollectionUtils.isNotEmpty(workerOffers)) {
                    taskDescriptions = scheduler.resourceOffers(workerOffers);
                }
            }

            if (CollectionUtils.isNotEmpty(taskDescriptions)) {
                launchTask(taskDescriptions);
            }
        }
        private void makeOffers(String executorId) {
            List<TaskDescription> taskDescriptions;
            synchronized (CoarseGrainedSchedulerBackend.this) {
                if (executorIsAlive(executorId)) {
                    ExecutorData executorData = executorDataMap.get(executorId);
                    List<WorkerOffer> workerOffers = Lists.newArrayList(
                            new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores)
                    );
                    taskDescriptions = scheduler.resourceOffers(workerOffers);
                } else {
                    taskDescriptions = Collections.emptyList();
                }
            }

            if (CollectionUtils.isNotEmpty(taskDescriptions)) {
                launchTask(taskDescriptions);
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
                        msg = format(msg, task.taskId, task.index, buffer.limit(), maxRpcMessageSize);
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

        /**********************************删除Executor(连接断开)*********************************/
        private void removeExecutor(String executorId, ExecutorLossReason reason) {
            ExecutorData executorData = executorDataMap.get(executorId);
            if (executorData != null) {
                synchronized (CoarseGrainedSchedulerBackend.this) {
                    addressToExecutorId.remove(executorData.executorAddress);
                    executorDataMap.remove(executorId);
                    executorsPendingLossReason.remove(executorId);
                    executorsPendingToRemove.remove(executorId);
                }

                totalCoreCount.addAndGet(executorData.totalCores);
                totalRegisteredExecutors.addAndGet(-1);
                scheduler.executorLost(executorId, reason);
                listenerBus.post(new SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason));
            } else {
                // SPARK-15262: If an executor is still alive even after the scheduler has removed
                // its metadata, we may receive a heartbeat from that executor and tell its block
                // manager to reregister itself. If that happens, the block manager master will know
                // about the executor, but the scheduler will not. Therefore, we should remove the
                // executor from the block manager when we hit this case.
                scheduler.sc.env.blockManager.master.removeExecutorAsync(executorId);
            }
        }

        /********************************删除Spark Worker******************************/
        private void removeWorker(String workerId, String host, String reason) {
            LOGGER.info("移除Spark Worker(workerId = {}, host = {}), 原因: {}", workerId, host, reason);
            scheduler.workerRemoved(workerId, host, reason);
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
        Map<String, String> properties = Maps.newHashMap();
        scheduler.sc.conf.getAll().forEach((key, value) -> {
            if (key.startsWith("spark.")) {
                properties.put(key, value);
            }
        });
        driverEndpoint = rpcEnv.setRpcEndPointRef(ENDPOINT_NAME,
                                                  createDriverEndPoint(properties));
    }

    private void stopExecutors() {
        if (driverEndpoint != null) {
            LOGGER.info("Shutting down all executors");
            try {
                driverEndpoint.askSync(new StopExecutors());
            } catch (Exception e) {
                throw new SparkException("Error asking standalone scheduler to shut down executors", e);
            }
        }
    }

    @Override
    public void stop() {
        stopExecutors();
        try {
            if (driverEndpoint != null) {
                driverEndpoint.askSync(new StopDriver());
            }
        } catch (Exception e) {
            throw new SparkException("Error stopping standalone scheduler's driver endpoint", e);
        }
    }

    /**
     * Reset the state of CoarseGrainedSchedulerBackend to the initial state. Currently it will only
     * be called in the yarn-client mode when AM re-registers after a failure.
     * */
    protected void reset() {
        Set<String> executors;
        synchronized (this) {
            requestedTotalExecutors = 0;
            numPendingExecutors = 0;
            executorsPendingToRemove.clear();
            executors = executorDataMap.keySet();
        }

        // Remove all the lingering executors that should be removed but not yet. The reason might be
        // because (1) disconnected event is not yet received; (2) executors die silently.
        executors.forEach(execId -> removeExecutor(execId,
                                                   new SlaveLost("Stale executor after cluster manager re-registered.")));
    }

    @Override
    public void reviveOffers() {
        driverEndpoint.send(new ReviveOffers());
    }

    @Override
    public void killTask(long taskId, String executorId, boolean interruptThread, String reason) {
        driverEndpoint.send(new KillTask(taskId, executorId, interruptThread, reason));
    }

    /**
     * Called by subclasses when notified of a lost worker. It just fires the message and returns
     * at once.
     * */
    protected void removeExecutor(String executorId, ExecutorLossReason reason) {
        driverEndpoint.ask(new RemoveExecutor(executorId, reason))
                      .exceptionally(t -> {
                          LOGGER.error(t.getMessage(), t);
                          return t;
                      });
    }

    protected void removeWorker(String workerId, String host, String reason) {
        driverEndpoint.ask(new RemoveWorker(workerId, host, reason))
                      .exceptionally(t -> {
                          LOGGER.error(t.getMessage(), t);
                          return t;
                      });
    }

    public boolean sufficientResourcesRegistered() {
        return true;
    }

    @Override
    public boolean isReady() {
        if (sufficientResourcesRegistered()) {
            LOGGER.info("SchedulerBackend is ready for scheduling beginning after reached " +
                        "minRegisteredResourcesRatio: {}", minRegisteredRatio);
            return true;
        }
        if ((System.currentTimeMillis() - createTime) >- maxRegisteredWaitingTimeMs) {
            LOGGER.info("SchedulerBackend is ready for scheduling beginning after waiting " +
                        "maxRegisteredResourcesWaitingTime: {}(ms)", maxRegisteredWaitingTimeMs);
            return true;
        }
        return false;
    }

    @Override
    public List<String> getExecutorIds() {
        return Lists.newLinkedList(executorDataMap.keySet());
    }

    @Override
    public boolean requestTotalExecutors(int numExecutors, int localityAwareTasks, Map<String, Integer> hostToLocalTaskCount) {
        if (numExecutors < 0) {
            throw new IllegalArgumentException(format("Attempted to request a negative number of executor(s) " +
                                                      "%d from the cluster manager. Please specify a positive number!", numExecutors));
        }

        Future<Boolean> response;
        synchronized (this) {
            this.requestedTotalExecutors = numExecutors;
            this.localityAwareTasks = localityAwareTasks;
            this.hostToLocalTaskCount = hostToLocalTaskCount;

            int numExistingExecutors = executorDataMap.size();
            numPendingExecutors = Math.max(numExecutors - numExistingExecutors + executorsPendingToRemove.size(), 0);

            response = doRequestTotalExecutors(numExecutors);
        }

        return getFutureResult(response);
    }

    @Override
    public boolean requestExecutors(int numAdditionalExecutors) {
        if (numAdditionalExecutors < 0) {
            throw new IllegalArgumentException(format("Attempted to request a negative number of additional executor(s) " +
                                                      "%d from the cluster manager. Please specify a positive number!", numAdditionalExecutors));

        }

        LOGGER.info("Requesting {} additional executor(s) from the cluster manager", numAdditionalExecutors);

        Future<Boolean> response;
        synchronized (this) {
            requestedTotalExecutors += numAdditionalExecutors;
            numPendingExecutors += numAdditionalExecutors;
            int numExistingExecutors = executorDataMap.size();

            LOGGER.debug("Number of pending executors is now {}", numPendingExecutors);

            if (requestedTotalExecutors !=
                    (numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size())) {
                LOGGER.debug("requestExecutors({}) : Executor request doesn't match: \n" +
                             "requestedTotalExecutors   = {} \n" +
                             "numExistingExecutors      = {} \n" +
                             "numPendingExecutors       = {}\n" +
                             "executorsPendingToRemove  = {}", numAdditionalExecutors, requestedTotalExecutors,
                                                               numExistingExecutors, numPendingExecutors,
                                                               executorsPendingToRemove.size());
            }
            response = doRequestTotalExecutors(numAdditionalExecutors);
        }
        return getFutureResult(response);
    }

    @Override
    public List<String> killExecutors(List<String> executorIds, boolean replace, boolean force) {
        LOGGER.info("Requesting to kill executor(s): {}", join(executorIds, ", "));

        Future<Boolean> killExecutors;
        synchronized (this) {
            Tuple2<List<String>, List<String>> t = partition(executorIds, executorDataMap::containsKey);

            t._2().forEach(execId -> LOGGER.info("Executor to kill {} does not exist!", execId));

            List<String> executorsToKill = t._1().stream()
                                                 .filter(execId -> !executorsPendingToRemove.containsKey(execId))
                                                 .filter(execId -> force || !scheduler.isExecutorBusy(execId))
                                                 .collect(Collectors.toList());

            LOGGER.info("Actual list of executor(s) to be killed is {}", join(executorsToKill, ", "));

            executorsToKill.forEach(execId -> executorsPendingToRemove.put(execId, !replace));

            // If we do not wish to replace the executors we kill, sync the target number of executors
            // with the cluster manager to avoid allocating new ones. When computing the new target,
            // take into account executors that are pending to be added or removed.
            CompletableFuture<Boolean> adjustTotalExecutors;
            if (!replace) {
                requestedTotalExecutors = Math.max(requestedTotalExecutors - executorsToKill.size(), 0);
                adjustTotalExecutors = doRequestTotalExecutors(requestedTotalExecutors);
            } else {
                numPendingExecutors += t._1().size();
                adjustTotalExecutors = CompletableFuture.completedFuture(true);
            }

            CompletableFuture<Boolean> killResponse = adjustTotalExecutors.thenApplyAsync(reqResp -> {
                if (CollectionUtils.isNotEmpty(executorsToKill)) {
                    try {
                        CompletableFuture<Boolean> f = doKillExecutors(executorsToKill);
                        return f.get();
                    } catch (Exception e) {
                        throw new SparkException("kill executor (" + join(executorsToKill, ", ") + ") failure", e);
                    }
                }
                return true;
            });

            boolean killResp = getFutureResult(killResponse);

            return killResp ? executorsToKill : Collections.emptyList();
        }
    }


    @Override
    public boolean killExecutorsOnHost(String host) {
        driverEndpoint.send(new KillExecutorsOnHost(host));
        return true;
    }

    public abstract CompletableFuture<Boolean> doRequestTotalExecutors(int requestedTotal);

    public abstract CompletableFuture<Boolean> doKillExecutors(List<String> executorIds);
}
