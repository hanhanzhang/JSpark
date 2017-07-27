package com.sdu.spark.scheduler.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.ExecutorAllocationClient;
import com.sdu.spark.rpc.*;
import com.sdu.spark.scheduler.*;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerExecutorAdded;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerExecutorRemoved;
import com.sdu.spark.scheduler.cluster.CoarseGrainedClusterMessage.*;
import com.sdu.spark.utils.DefaultFuture;
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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.sdu.spark.utils.RpcUtils.maxMessageSizeBytes;
import static com.sdu.spark.utils.ThreadUtils.newDaemonSingleThreadScheduledExecutor;
import static com.sdu.spark.utils.Utils.getFutureResult;

/**
 * {@link CoarseGrainedSchedulerBackend}职责:
 *
 * 1: 启动DriverEndPoint({@link #start()})
 *
 *    监听Executor任务运行状态(当Executor运行完成任务, TaskScheduler调度任务)
 *
 * 2: Executor资源管理
 *
 *  1': 向Spark Master申请Executor资源
 *
 *  2': 请求Spark Master关闭Executor
 *
 * @author hanhan.zhang
 * */
public abstract class CoarseGrainedSchedulerBackend implements ExecutorAllocationClient, SchedulerBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoarseGrainedSchedulerBackend.class);

    public static String ENDPOINT_NAME = "CoarseGrainedScheduler";

    protected SparkConf conf;
    private RpcEnv rpcEnv;
    private LiveListenerBus listenerBus;
    private int maxRpcMessageSize;


    /*******************************Spark Job分配Executor资源管理******************************/
    protected TaskSchedulerImpl scheduler;
    // Spark Job分配CPU核数
    private AtomicInteger totalCoreCount = new AtomicInteger(0);
    // Spark Job分配Executor
    private int requestedTotalExecutors = 0;
    // Spark Job等待分配Executor
    private int numPendingExecutors = 0;
    // Spark Job注册Executor数
    private AtomicInteger totalRegisteredExecutors = new AtomicInteger(0);
    private int localityAwareTasks = 0;
    private Map<String, Integer> hostToLocalTaskCount = Collections.emptyMap();
    // Spark Job分配Executor[key = executorId, value = ExecutorData]
    private Map<String, ExecutorData> executorDataMap = Maps.newHashMap();
    // 待删除Executor
    private Map<String, Boolean> executorsPendingToRemove = Maps.newHashMap();

    /*********************************Spark Job Driver***********************************/
    private RpcEndPointRef driverEndpoint;

    public CoarseGrainedSchedulerBackend(TaskSchedulerImpl scheduler) {
        this.scheduler = scheduler;
        this.conf = this.scheduler.sc.conf;
        this.rpcEnv = this.scheduler.sc.env.rpcEnv;
        this.listenerBus = this.scheduler.sc.listenerBus;
        this.maxRpcMessageSize = maxMessageSizeBytes(conf);
    }

    private RpcEndPoint createDriverEndPoint() {
        return new DriverEndPoint(this.rpcEnv);
    }

    private class DriverEndPoint extends RpcEndPoint {
        protected Set<String> executorsPendingLossReason = Sets.newHashSet();
        // Executor分配地址映射[key = RpcAddress, value = executorId]
        private Map<RpcAddress, String> addressToExecutorId = Maps.newHashMap();
        // 定时调度Task分配给Executor线程
        private ScheduledExecutorService reviveThread = newDaemonSingleThreadScheduledExecutor("driver-receive-thread");

        public DriverEndPoint(RpcEnv rpcEnv) {
            super(rpcEnv);
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
        public void onDisconnect(RpcAddress rpcAddress) {
            String executorId = addressToExecutorId.get(rpcAddress);
            String reason = String.format("RpcAddress(host = %s, port = %d)断开连接", rpcAddress.host, rpcAddress.port);
            removeExecutor(executorId, reason);
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
                executor.executorRef.send(new RegisterExecutorFailed(String.format("Executor is blacklisted: %s", executor.executorId)));
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

        /**********************************删除Executor(连接断开)*********************************/
        private void removeExecutor(String executorId, String reason) {
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
        this.driverEndpoint = rpcEnv.setRpcEndPointRef(ENDPOINT_NAME, createDriverEndPoint());
    }

    private boolean stopExecutors() {
        if (driverEndpoint != null) {
            LOGGER.info("关闭全部Executor");
            try {
                return (boolean) driverEndpoint.askSync(new StopExecutors(), 1000);
            } catch (Exception e) {
                LOGGER.error("关闭Executor异常", e);
                return false;
            }
        }
        return false;
    }

    @Override
    public void stop() {
        stopExecutors();
        if (driverEndpoint != null) {
            driverEndpoint.ask(new StopDriver());
        }
    }

    @Override
    public void reviveOffers() {
        driverEndpoint.send(new ReviveOffers());
    }

    @Override
    public void killTask(long taskId, String executorId, boolean interruptThread, String reason) {
        driverEndpoint.send(new KillTask(taskId, executorId, interruptThread, reason));
    }

    @Override
    public List<String> getExecutorIds() {
        return Lists.newLinkedList(executorDataMap.keySet());
    }

    @Override
    public boolean requestTotalExecutors(int numExecutors, int localityAwareTasks, Map<String, Integer> hostToLocalTaskCount) {
        if (numExecutors < 0) {
            throw new IllegalArgumentException("尝试申请Executor: " + numExecutors);
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
            throw new IllegalArgumentException("尝试申请Executor: " + numAdditionalExecutors);
        }

        Future<Boolean> response;
        synchronized (this) {
            requestedTotalExecutors += numAdditionalExecutors;
            numPendingExecutors += numAdditionalExecutors;
            int numExistingExecutors = executorDataMap.size();

            if (requestedTotalExecutors != numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size()) {
                LOGGER.debug("尝试请求Executor(num = {}), Executor分配不匹配: \n" +
                             "requestedTotalExecutors = {} \n" +
                             "numExistingExecutors = {} \n" +
                             "numPendingExecutors = {}\n" +
                             "executorsPendingToRemove = {}", numAdditionalExecutors, requestedTotalExecutors,
                                                              numExistingExecutors, numPendingExecutors,
                                                              executorsPendingToRemove.size());
            }
            response = doRequestTotalExecutors(numAdditionalExecutors);
        }
        return getFutureResult(response);
    }

    @Override
    public List<String> killExecutors(List<String> executorIds, boolean replace, boolean force) {
        Future<Boolean> killExecutors;
        synchronized (this) {
            List<String> knownExecutors = Lists.newLinkedList();
            List<String> unknownExecutors = Lists.newLinkedList();
            executorIds.forEach(execId -> {
                if (executorDataMap.containsKey(execId)) {
                    knownExecutors.add(execId);
                } else {
                    unknownExecutors.add(execId);
                }
            });

            unknownExecutors.forEach(execId -> LOGGER.info("需关闭的Executor(execId = {})不存在", execId));

            List<String> executorsToKill = knownExecutors.stream().filter(execId -> !executorsPendingToRemove.containsKey(execId))
                                                                  .filter(execId -> force || !scheduler.isExecutorBusy(execId))
                                                                  .collect(Collectors.toList());
            executorsToKill.forEach(execId -> executorsPendingToRemove.put(execId, !replace));

            if (!replace) {
                requestedTotalExecutors = Math.max(requestedTotalExecutors - executorsToKill.size(), 0);
                doRequestTotalExecutors(requestedTotalExecutors);
            } else {
                numPendingExecutors += knownExecutors.size();
            }


            if (CollectionUtils.isNotEmpty(executorsToKill)) {
                killExecutors = doKillExecutors(executorsToKill);
            } else {
                killExecutors = new DefaultFuture<>(true);
            }

            boolean response = getFutureResult(killExecutors);

            return response ? executorsToKill : Collections.emptyList();
        }
    }


    @Override
    public boolean killExecutorsOnHost(String host) {
        driverEndpoint.send(new KillExecutorsOnHost(host));
        return true;
    }

    protected void removeExecutor(String executorId, String reason) {
        try {
            boolean success = (boolean) driverEndpoint.askSync(new RemoveExecutor(executorId, reason), 1000);
            if (success) {
                LOGGER.error("移除Executor[execId = {}]成功, 原因: {}", executorId, reason);
            }
        } catch (Exception e) {
            LOGGER.error("移除Executor[execId = {}]异常", executorId, e);
        }

    }

    protected void removeWorker(String workerId, String host, String reason) {
        LOGGER.info("移除Worker[workerId = {}, host = {}], 原因: {}", workerId, host, reason);
        driverEndpoint.ask(new RemoveWorker(workerId, host, reason));
    }

    public abstract Future<Boolean> doRequestTotalExecutors(int requestedTotal);

    public abstract Future<Boolean> doKillExecutors(List<String> executorIds);
}
