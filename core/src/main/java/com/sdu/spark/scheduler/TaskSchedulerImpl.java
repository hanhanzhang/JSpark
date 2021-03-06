package com.sdu.spark.scheduler;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.ExecutorAllocationClient;
import com.sdu.spark.SparkContext;
import com.sdu.spark.scheduler.TaskEndReason.*;
import com.sdu.spark.executor.ExecutorExitCode.ExecutorLossReason;
import com.sdu.spark.executor.ExecutorExitCode.SlaveLost;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.SchedulableBuilder.FIFOSchedulableBuilder;
import com.sdu.spark.scheduler.SchedulableBuilder.FairSchedulableBuilder;
import com.sdu.spark.storage.BlockManagerId;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * TaskSchedulerImpl
 *
 * @author hanhan.zhang
 * */
public class TaskSchedulerImpl implements TaskScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskSchedulerImpl.class);

    public static final String SCHEDULER_MODE_PROPERTY = "spark.scheduler.mode";


    public SparkContext sc;
    private SparkConf conf;
    public int CPUS_PER_TASK;
    private boolean isLocal;



    private SchedulingMode schedulingMode;
    private SchedulableBuilder schedulableBuilder;
    private Pool rootPool;

    private Timer starvationTimer = new Timer(true);

    /********************************Spark Task***********************************/
    // key = stateId, value = [key = , value = ]
    private Map<Integer, Map<Integer, TaskSetManager>> taskSetsByStageIdAndAttempt = Maps.newHashMap();
    public Map<Long, TaskSetManager> taskIdToTaskSetManager = Maps.newHashMap();
    private Map<Long, String> taskIdToExecutorId = Maps.newHashMap();
    private Map<String, Set<Long>> executorIdToRunningTaskIds = Maps.newHashMap();
    private volatile boolean hasReceivedTask = false;
    private volatile boolean hasLaunchedTask = false;
    private AtomicLong nextTaskId = new AtomicLong(0);
    // Spark Task失败重试次数
    private int maxTaskFailures;
    public DAGScheduler dagScheduler = null;
    private long STARVATION_TIMEOUT_MS;
    private TaskResultGetter taskResultGetter;


    /*****************************Spark Executor**********************************/
    public Map<String, Set<String>> hostToExecutors = Maps.newHashMap();
    private Map<String, String> executorIdToHost = Maps.newHashMap();
    private SchedulerBackend backend;
    // Lazily initializing blackListTrackOpt to avoid getting empty ExecutorAllocationClient,
    // because ExecutorAllocationClient is created after this TaskSchedulerImpl.
    private BlacklistTracker blacklistTrackerOpt;


    public TaskSchedulerImpl(SparkContext sc) {
        this(sc, sc.conf.getInt("spark.task.maxFailures", 1));
    }

    public TaskSchedulerImpl(SparkContext sc, int maxTaskFailures) {
        this(sc, maxTaskFailures, false);
    }

    public TaskSchedulerImpl(SparkContext sc, int maxTaskFailures, boolean isLocal) {
        this.sc = sc;
        this.conf = this.sc.conf;
        this.CPUS_PER_TASK = this.conf.getInt("spark.task.cpus", 1);
        this.STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s");
        this.maxTaskFailures = maxTaskFailures;
        this.isLocal = isLocal;

        this.schedulingMode = SchedulingMode.withName(this.conf.get(SCHEDULER_MODE_PROPERTY, SchedulingMode.FIFO.name()));
        this.rootPool = new Pool("", schedulingMode, 0, 0);
        this.taskResultGetter = new TaskResultGetter(sc.env, this);
    }

    public void initialize(SchedulerBackend schedulerBackend) {
        this.backend = schedulerBackend;
        switch (schedulingMode) {
            case FAIR:
                schedulableBuilder = new FairSchedulableBuilder(rootPool, conf);
                schedulableBuilder.buildPools();
                break;
            case FIFO:
                schedulableBuilder = new FIFOSchedulableBuilder(rootPool);
                schedulableBuilder.buildPools();
                break;
            default:
                throw new IllegalArgumentException("Unsupported schedule mode : " + schedulingMode);
        }
    }

    @Override
    public void start() {
        this.backend.start();
        this.blacklistTrackerOpt = maybeCreateBlacklistTracker(sc);
    }

    @Override
    public void postStartHook() {
        waitBackendReady();
    }
    private void waitBackendReady() {
        if (backend.isReady()) {
            return;
        }

        while (!backend.isReady()) {
            if (sc.stopped.get()) {
                throw new IllegalStateException("Spark context stopped while waiting for backend");
            }
            synchronized (this) {
                try {
                    this.wait(100);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    /*********************************Spark Submit Task*********************************/
    @Override
    public void submitTasks(TaskSet taskSet) {
        synchronized (this) {
            TaskSetManager manager = createTaskSetManager(taskSet, maxTaskFailures);
            int stage = taskSet.stageId;
            Map<Integer, TaskSetManager> stageTaskSets =  taskSetsByStageIdAndAttempt.get(stage);
            if (stageTaskSets == null) {
                stageTaskSets = Maps.newHashMap();
                taskSetsByStageIdAndAttempt.put(stage, stageTaskSets);
            }
            stageTaskSets.put(taskSet.stageAttemptId, manager);

            boolean conflictingTaskSet = false;
            for (Map.Entry<Integer, TaskSetManager> entry : stageTaskSets.entrySet()) {
                TaskSetManager ts = entry.getValue();
                if (ts.taskSet != taskSet && ts.isZombie) {
                    conflictingTaskSet = true;
                    break;
                }
            }
            if (conflictingTaskSet) {
                throw new IllegalStateException(format("Stage[id = %s]超过两个TaskSet: %s", stage,
                                                StringUtils.join(stageTaskSets.values(), '\n')));
            }

            schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties);
            if (!isLocal && !hasReceivedTask) {
                starvationTimer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        if (!hasLaunchedTask) {

                        } else {
                            this.cancel();
                        }
                    }
                }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS);
            }
            hasReceivedTask = true;
        }
        backend.reviveOffers();

    }
    private TaskSetManager createTaskSetManager(TaskSet taskSet, int maxTaskFailures) {
        return new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt);
    }

    public synchronized void handleTaskGettingResult(TaskSetManager taskSetManager, long tid) {
        taskSetManager.handleTaskGettingResult(tid);
    }

    @Override
    public Pool rootPool() {
        return rootPool;
    }

    @Override
    public void setDAGScheduler(DAGScheduler dagScheduler) {
        this.dagScheduler = dagScheduler;
    }

    @Override
    public SchedulingMode schedulingMode() {
        return schedulingMode;
    }

    @Override
    public void stop() {

    }

    @Override
    public void cancelTasks(int stageId, boolean interruptThread) {

    }

    @Override
    public boolean killTaskAttempt(int taskId, boolean interruptThread, String reason) {
        return false;
    }

    @Override
    public void defaultParallelism() {

    }

    /**
     * Return true if the driver knows about the given block manager. Otherwise, return false,
     * indicating that the block manager should re-register.
     * */
    @Override
    public boolean executorHeartbeatReceived(String execId, BlockManagerId blockManagerId) {
        // TODO: Task Metrics
        return dagScheduler.executorHeartbeatReceived(execId, blockManagerId);
    }

    @Override
    public void executorLost(String executorId, ExecutorLossReason reason) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void workerRemoved(String workerId, String host, String message) {

    }

    @Override
    public String applicationAttemptId() {
        return null;
    }

    /*****************************Spark Job Task运行状态变更******************************/
    public void statusUpdate(long taskId, TaskState state, ByteBuffer value) {
        String failedExecutor = null;
        ExecutorLossReason reason = null;
        synchronized (this) {
            try {
                TaskSetManager taskSet = taskIdToTaskSetManager.get(taskId);
                if (taskSet == null) {
                    LOGGER.warn("Ignoring update with state {} for TID {} because its task set is gone (this is " +
                                "likely the result of receiving duplicate task finished status updates) or its " +
                                "executor has been marked as failed.", state, taskId);
                } else {
                    if (state == TaskState.LOST) {
                        String execId = taskIdToExecutorId.get(taskId);
                        if (isNullOrEmpty(execId)) {
                            throw new IllegalStateException("taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)");
                        }
                        if (executorIdToRunningTaskIds.containsKey(execId)) {
                            reason = new SlaveLost(format("Task %s was lost, so marking the executor as lost as well.", taskId));
                            removeExecutor(execId, reason);
                            failedExecutor = execId;
                        }
                    }
                    if (TaskState.isFinished(state)) {
                        cleanupTaskState(taskId);
                        taskSet.removeRunningTask(taskId);
                        if (state == TaskState.FINISHED) {
                            taskResultGetter.enqueueSuccessfulTask(taskSet,
                                                                   taskId,
                                                                   value);
                        } else if (Sets.newHashSet(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                            taskResultGetter.enqueueFailedTask(taskSet,
                                                               taskId,
                                                               state,
                                                               value);
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Exception in statusUpdate", e);
            }
        }

        // Update the DAGScheduler without holding a lock on this, since that can deadlock
        if (failedExecutor != null) {
            dagScheduler.executorLost(failedExecutor, reason);
            backend.reviveOffers();
        }
    }

    public synchronized void handleFailedTask(TaskSetManager taskSetManager,
                                              long tid,
                                              TaskState taskState,
                                              TaskFailedReason reason) {
        taskSetManager.handleFailedTask(tid, taskState, reason);
        if (!taskSetManager.isZombie && !taskSetManager.someAttemptSucceeded(tid)) {
            // Need to revive offers again now that the task set manager state has been updated to
            // reflect failed tasks that need to be re-run.
            backend.reviveOffers();
        }
    }

    public synchronized void handleSuccessfulTask(TaskSetManager taskSetManager,
                                                  long tid,
                                                  DirectTaskResult<?> taskResult) {
        taskSetManager.handleSuccessfulTask(tid, taskResult);
    }

    /******************************Spark Job Task分发***********************************/
    public List<TaskDescription> resourceOffers(List<WorkerOffer> offers) {
        throw new UnsupportedOperationException("");
    }

    private void cleanupTaskState(long tid) {
        taskIdToTaskSetManager.remove(tid);
        String execId = taskIdToExecutorId.remove(tid);
        if (isNotEmpty(execId)) {
            Set<Long> runningTaskIds = executorIdToRunningTaskIds.get(execId);
            if (CollectionUtils.isNotEmpty(runningTaskIds)) {
                runningTaskIds.remove(tid);
            }
        }
    }

    /******************************Spark Executor运行状态********************************/
    public boolean isExecutorBusy(String executorId) {
        throw new UnsupportedOperationException("");
    }

    private void removeExecutor(String execId, ExecutorLossReason reason) {

    }

    public void error(String message) {
        throw new UnsupportedOperationException("");
    }

    /**
     * Get a snapshot of the currently blacklisted nodes for the entire application.  This is
     * thread-safe -- it can be called without a lock on the TaskScheduler.
     */
    public Set<String> nodeBlacklist() {
        if (blacklistTrackerOpt != null) {
            return blacklistTrackerOpt.nodeBlacklist.get();
        }
        return Collections.emptySet();
    }


    private BlacklistTracker maybeCreateBlacklistTracker(SparkContext sc) {
        if (BlacklistTracker.isBlacklistEnabled(sc.conf)) {
            if (sc.schedulerBackend instanceof ExecutorAllocationClient) {
                return new BlacklistTracker(sc, (ExecutorAllocationClient) sc.schedulerBackend);
            }
        }
        return null;
    }

    private long newTaskId() {
        return nextTaskId.getAndIncrement();
    }
}
