package com.sdu.spark.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.*;
import com.sdu.spark.broadcast.Broadcast;
import com.sdu.spark.executor.ExecutorExitCode.ExecutorLossReason;
import com.sdu.spark.executor.ExecutorExitCode.SlaveLost;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.scheduler.DAGSchedulerEvent.*;
import com.sdu.spark.scheduler.JobResult.JobFailed;
import com.sdu.spark.scheduler.SparkListenerEvent.*;
import com.sdu.spark.scheduler.TaskEndReason.*;
import com.sdu.spark.scheduler.action.JobAction;
import com.sdu.spark.scheduler.action.ResultHandler;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockId.RDDBlockId;
import com.sdu.spark.storage.BlockManagerId;
import com.sdu.spark.storage.BlockManagerMaster;
import com.sdu.spark.storage.BlockManagerMessages.BlockManagerHeartbeat;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.Clock;
import com.sdu.spark.utils.Clock.SystemClock;
import com.sdu.spark.utils.EventLoop;
import com.sdu.spark.utils.SparkDriverExecutionException;
import com.sdu.spark.utils.scala.Tuple2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.NotSerializableException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.sdu.spark.SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL;
import static com.sdu.spark.network.utils.JavaUtils.bufferToArray;
import static com.sdu.spark.utils.Utils.exceptionString;
import static com.sdu.spark.utils.Utils.getFormattedClassName;
import static org.apache.commons.lang3.BooleanUtils.toBoolean;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * DAGSchedule负责接收由RDD构成的DAG并将RDD划分到不同Stage. 根据Stage的不同类型(目前只有ShuffleMapStage和ResultStage)为
 * Stage中RDD未完成的Partition创建Task(目前有ShuffleMapTask和ResultTask). 最后DAGSchedule将每个Stage中的Task以任务集
 * 合(TaskSet)的形式提交给TaskSchedule处理.
 *
 * 1: {@link #runJob(RDD, JobAction, List, ResultHandler, Properties)}划分并提及Stage
 *
 *  DAGScheduler.runJob()
 *    |
 *    +----> DAGScheduler.submitJob()[向EventLoop投递JobSubmitEvent, 返回JobWaiter(Job运行结果)]
 *
 *  DAGScheduler.handleJobSubmitted()[EventLoop处理JobSubmitEvent事件]
 *    |
 *    +----> DAGScheduler.createResultStage()[根据DAG Final RDD生成ResultStage]
 *              |
 *              +----> DAGScheduler.submitStage()[提交ResultStage]
 *
 *  Note:
 *
 *    DAGScheduler划分Stage过程主要有:
 *
 *    1': 由final rdd构建ResultStage, 构建ResultStage过程中, 根据final rdd的ShuffleDependency构建ResultStage依赖的
 *
 *        ShuffleMapStage, 递归构建ShuffleMapStage依赖的ShuffleMapStage[递归结束条件: rdd.dependencies() == null]
 *
 *    2': Stage划分中需维护StageId与Stage、ShuffleId与ShuffleMapStage、jobId与Stage映射关系, 此外MapOutputTrackerMaster
 *
 *        维护ShuffleId与parent rdd的分区数映射关系
 *
 *    3':
 *
 * 2: {@link #cancelJob(int, String)}终止作业及Job关联的Stage信息
 *
 * Note:
 *
 *  DAGScheduler只运行在Driver端
 *
 * @author hanhan.zhang
 * */
@SuppressWarnings("unchecked")
public class DAGScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DAGScheduler.class);

    private SparkContext sc;
    private SerializerInstance closureSerializer;
    private TaskScheduler taskScheduler;
    private LiveListenerBus listenerBus;
    private OutputCommitCoordinator outputCommitCoordinator;
    private MapOutputTrackerMaster mapOutputTracker;
    private BlockManagerMaster blockManagerMaster;
    private SparkEnv env;
    private Clock clock;

    private AtomicInteger nextJobId = new AtomicInteger(0);
    // key = stageId, value = ShuffleMapStage
    private Map<Integer, ShuffleMapStage> shuffleIdToMapStage = Maps.newHashMap();
    private AtomicInteger nextStageId = new AtomicInteger(0);
    private Map<Integer, Stage> stageIdToStage = Maps.newHashMap();
    private Set<Stage> waitingStages = Sets.newHashSet();
    private Set<Stage> runningStages = Sets.newHashSet();
    private Set<Stage> failedStages = Sets.newHashSet();
    // JobId与Stage对应关系[runJob生成jobId并划分jobId依赖的所有Stage]
    private Map<Integer, Set<Integer>> jobIdToStageIds = Maps.newHashMap();
    private Map<Integer, ActiveJob> jobIdToActiveJob = Maps.newHashMap();
    private Set<ActiveJob> activeJobs = Sets.newHashSet();
    // key = rdd标识, value = [分区][当前分区任务运行位置](即: 行表示分区, 列表示任务运行位置)
    private final Map<Integer, TaskLocation[][]> cacheLocs = Maps.newHashMap();

    // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
    // every task. When we detect a node failing, we note the current epoch number and failed
    // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
    //
    // TODO: Garbage collect information about failure epochs when we know there are no more
    //       stray messages to detect.
    private Map<String, Long> failedEpoch = Maps.newHashMap();

    // DAGEvent消息处理
    private DAGSchedulerEventProcessLoop eventProcessLoop;

    public DAGScheduler(SparkContext sc, TaskScheduler taskScheduler) {
        this(sc, taskScheduler, sc.listenerBus, (MapOutputTrackerMaster) sc.env.mapOutputTracker, sc.env.blockManager.master, sc.env);
    }

    public DAGScheduler(SparkContext sc) {
        this(sc, sc.taskScheduler);
    }

    public DAGScheduler(SparkContext sc, TaskScheduler taskScheduler, LiveListenerBus listenerBus,
                        MapOutputTrackerMaster mapOutputTracker, BlockManagerMaster blockManagerMaster,
                        SparkEnv env) {
        this(sc, taskScheduler, listenerBus, mapOutputTracker, blockManagerMaster, env, new SystemClock());
    }

    public DAGScheduler(SparkContext sc,
                        TaskScheduler taskScheduler,
                        LiveListenerBus listenerBus,
                        MapOutputTrackerMaster mapOutputTracker,
                        BlockManagerMaster blockManagerMaster,
                        SparkEnv env,
                        Clock clock) {
        this.sc = sc;
        this.taskScheduler = taskScheduler;
        this.listenerBus = listenerBus;
        this.mapOutputTracker = mapOutputTracker;
        this.blockManagerMaster = blockManagerMaster;
        this.env = env;
        this.outputCommitCoordinator = this.env.outputCommitCoordinator;
        this.closureSerializer = this.env.closureSerializer.newInstance();
        this.clock = clock;
        // 初始化
        this.eventProcessLoop = new DAGSchedulerEventProcessLoop(this);
        this.eventProcessLoop.start();
        this.taskScheduler.setDAGScheduler(this);
    }

    /********************************Spark Executor心跳消息**************************/
    /**
     * Return true if the driver knows about the given block manager. Otherwise, return false,
     * indicating that the block manager should re-register.
     * */
    public boolean executorHeartbeatReceived(String execId, BlockManagerId blockManagerId) {
        // TODO: Executor Metrics
        // Driver BlockManagerMaster注册Executor端的BlockManager
        try {
            return (boolean) blockManagerMaster.driverEndpoint.askSync(new BlockManagerHeartbeat(blockManagerId));
        } catch (Exception e) {
            throw new SparkException("Ask BlockManagerHeartbeat failure", e);
        }
    }

    public void executorLost(String execId, ExecutorLossReason reason) {
        eventProcessLoop.post(new ExecutorLost(execId, reason));
    }

    /********************************Spark DAG State调度*********************************/
    /**
     * @param rdd target RDD to run tasks on
     * @param jobAction a function to run on each partition of the RDD
     * @param partitions set of partitions to run on; some jobs may not want to compute on all
     *                   partitions of the target RDD, e.g. for operations like first()
     * @param resultHandler callback to pass each result to
     * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
     * */
    public <T, U> void runJob(RDD<T> rdd,
                              JobAction<T, U> jobAction,
                              List<Integer> partitions,
                              ResultHandler<U> resultHandler,
                              Properties properties) throws Exception {
        long start = System.nanoTime();
        JobWaiter<U> waiter = submitJob(rdd, jobAction, partitions, resultHandler, properties);
        Future<Boolean> future = waiter.completionFuture();
        try {
            boolean success = future.get();
            if (success) {
                LOGGER.info("Job {} finished: {}, took {} s", waiter.jobId, (System.nanoTime() - start) / 1e9);
            }
        } catch (Exception e) {
            LOGGER.error("Job {} failed: {}, took {} s", waiter.jobId, (System.nanoTime() - start) / 1e9);
            // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
            StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
            StackTraceElement callerStackTrace = stackTraces[stackTraces.length - 1];
            StackTraceElement[] exceptionStackTrace = e.getStackTrace();
            e.setStackTrace(ArrayUtils.add(exceptionStackTrace, callerStackTrace));
            throw e;
        }
    }

    private <T, U> JobWaiter<U> submitJob(RDD<T> rdd,
                                          JobAction<T, U> jobAction,
                                          List<Integer> partitions,
                                          ResultHandler<U> resultHandler,
                                          Properties properties) {
        // 校验传入RDD分区数是否正确
        int maxPartitions = rdd.partitions().length;
        for (int partition : partitions) {
            if (partition < 0 || partition >= maxPartitions) {
                throw new IllegalArgumentException(String.format("Attempting to access a non-existent partition: %d. " +
                                                                 "Total number of partitions: %d", partition, maxPartitions));
            }
        }

        // 生成JobId
        int jobId = nextJobId.getAndIncrement();
        if (partitions.size() == 0) {
            return new JobWaiter<>(this, jobId, 0, resultHandler);
        }

        assert partitions.size() > 0;
        JobWaiter<U> waiter = new JobWaiter<>(this, jobId, partitions.size(), resultHandler);

        // 提交作业事件
        eventProcessLoop.post(new JobSubmitted<>(jobId,
                                                 rdd,
                                                 jobAction,
                                                 partitions,
                                                 waiter,
                                                 new Properties(properties)));
        return waiter;
    }

    private <T, U> void handleJobSubmitted(int jobId,
                                           RDD<T> finalRDD,
                                           JobAction<T, U> jobAction,
                                           List<Integer> partitions,
                                           JobListener listener,
                                           Properties properties) {
        ResultStage finalStage;
        try {
            // New stage creation may throw an exception if, for example, jobs are run on a
            // HadoopRDD whose underlying HDFS files have been deleted.
            finalStage = createResultStage(finalRDD, jobAction, partitions, jobId);
        } catch (Exception e){
            listener.jobFailed(e);
            return;
        }

        ActiveJob job = new ActiveJob(jobId, finalStage, listener, properties);
        clearCacheLocs();
        LOGGER.info("Got job {} ({}) with {} combiner partitions", job.jobId(),
                                                                   partitions.size());
        LOGGER.info("Final stage: {}({})", finalStage, finalStage.name);
        LOGGER.info("Parents of final stage: {}", finalStage.parents);
        LOGGER.info("Missing parents: {}", getMissingParentStages(finalStage));

        long jobSubmissionTime = clock.getTimeMillis();
        jobIdToActiveJob.put(jobId, job);
        activeJobs.add(job);
        finalStage.setActiveJob(job);
        Set<Integer> stageIds = jobIdToStageIds.get(jobId);
        List<StageInfo> stageInfos = stageIds.stream()
                                             .map(id -> stageIdToStage.get(id).latestInfo())
                                             .collect(Collectors.toList());
        listenerBus.post(new SparkListenerJobStart(job.jobId(),
                                                   jobSubmissionTime,
                                                   stageInfos,
                                                   properties));
        submitStage(finalStage);
    }

    private void clearCacheLocs() {
        synchronized (cacheLocs) {
            cacheLocs.clear();
        }
    }

    private void submitStage(Stage stage) {
        int jobId = activeJobForStage(stage);
        if (jobId != -1) {
            LOGGER.debug("submitStage({})", stage);
            if (!waitingStages.contains(stage) && !runningStages.contains(stage) && !failedStages.contains(stage)) {
                List<Stage> missing = getMissingParentStages(stage);
                // TODO: 按照划分来说应该按照升序
                Collections.sort(missing, (s1, s2) -> s1.id - s2.id);

                if (CollectionUtils.isEmpty(missing)) {
                    submitMissingTasks(stage, jobId);
                } else {
                    missing.forEach(this::submitStage);
                    waitingStages.add(stage);
                }
            }
        } else {
            abortStage(stage, "No active job for stage " + stage.id, null);
        }
    }

    private void abortStage(Stage failedStage, String reason, Throwable exception) {
        if (!stageIdToStage.containsKey(failedStage.id)) {
            // Skip all the actions if the stage has been removed.
            return;
        }

        List<ActiveJob> dependentJobs = activeJobs.stream()
                                                  .filter(job -> stageDependsOn(job.finalStage(), failedStage))
                                                  .collect(Collectors.toList());
        failedStage.latestInfo().setCompletionTime(clock.getTimeMillis());
        if (CollectionUtils.isNotEmpty(dependentJobs)) {
            dependentJobs.forEach(job -> failJobAndIndependentStages(
                    job,
                    String.format("Job aborted due to stage failure: %s", reason),
                    exception
            ));
        } else {
            LOGGER.info("Ignoring failure of {} because all jobs depending on it are done", failedStage);
        }
    }

    /**Return true if one of stage's ancestors is target.*/
    private boolean stageDependsOn(Stage stage, Stage target) {
        if (stage.equals(target)) {
            return true;
        }

        Set<RDD<?>> visitedRdds = Sets.newHashSet();
        Stack<RDD<?>> waitingForVisit = new Stack<>();

        waitingForVisit.push(stage.rdd);
        while (waitingForVisit.size() > 0) {
            RDD<?> rdd = waitingForVisit.pop();
            if (!visitedRdds.contains(rdd)) {
                visitedRdds.add(rdd);
                for (Dependency<?> dep : rdd.dependencies()) {
                    if (dep instanceof ShuffleDependency) {
                        ShuffleDependency<?, ?, ?> shuffleDep = (ShuffleDependency<?, ?, ?>) dep;
                        ShuffleMapStage mapStage = getOrCreateShuffleMapStage(shuffleDep, stage.firstJobId);
                        if (!mapStage.isAvailable()) {
                            waitingForVisit.push(mapStage.rdd);
                        }
                    } else if (dep instanceof NarrowDependency) {
                        NarrowDependency<?> narrowDep = (NarrowDependency<?>) dep;
                        waitingForVisit.push(narrowDep.rdd());
                    }
                }
            }
        }

        return visitedRdds.contains(target.rdd);
    }

    private void submitMissingTasks(Stage stage, int jobId) {
        LOGGER.debug("submitMissingTasks({})", stage);

        // First figure out the indexes of partition ids to compute.
        List<Integer> partitionsToCompute = stage.findMissingPartitions();

        // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
        // with this Stage
        Properties properties = jobIdToActiveJob.get(jobId).properties;

        runningStages.add(stage);
        if (stage instanceof ShuffleMapStage) {
            // 记录Stage运行状态信息
            outputCommitCoordinator.stageStart(stage.id, stage.numPartitions - 1);
        } else if (stage instanceof ResultStage) {
            outputCommitCoordinator.stageStart(stage.id, stage.rdd.partitions().length - 1);
        }

        // 计算作业运行位置
        Map<Integer, TaskLocation[]> taskIdToLocations = Maps.newHashMapWithExpectedSize(partitionsToCompute.size());
        try {
            if (stage instanceof ShuffleMapStage) {
                for (int partition : partitionsToCompute) {
                    TaskLocation[] taskLocations = getPreferredLocs(stage.rdd, partition);
                    taskIdToLocations.put(partition, taskLocations);
                }
            } else if (stage instanceof ResultStage) {
                for (int partition : partitionsToCompute) {
                    int p = ((ResultStage) stage).partitions.get(partition);
                    TaskLocation[] taskLocations = getPreferredLocs(stage.rdd, p);
                    taskIdToLocations.put(partition, taskLocations);
                }
            }
        } catch (Exception e) {
            stage.makeNewStageAttempt(partitionsToCompute.size());
            listenerBus.post(new SparkListenerEvent.SparkListenerStageSubmitted(stage.latestInfo(), properties));
            abortStage(stage, "Task creation failed: " + exceptionString(e), e);
            runningStages.remove(stage);
            return;
        }
        TaskLocation[][] taskLocations = new TaskLocation[partitionsToCompute.size()][];
        for (int partition : partitionsToCompute) {
            taskLocations[partition] = taskIdToLocations.get(partition);
        }
        stage.makeNewStageAttempt(partitionsToCompute.size(), taskLocations);

        // If there are tasks to execute, record the submission time of the stage. Otherwise,
        // post the even without the submission time, which indicates that this stage was
        // skipped.
        if (!partitionsToCompute.isEmpty()) {
            stage.latestInfo().setSubmissionTime(clock.getTimeMillis());
        }
        listenerBus.post(new SparkListenerStageSubmitted(stage.latestInfo(), properties));

        // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
        // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
        // the serialized copy of the RDD and for each task we will deserialize it, which means each
        // task gets a different copy of the RDD. This provides stronger isolation between tasks that
        // might modify state of objects referenced in their closures. This is necessary in Hadoop
        // where the JobConf/Configuration object is not thread-safe.
        Broadcast<byte[]> taskBinary = null;
        try {
            // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
            // For ResultTask, serialize and broadcast (rdd, func).
            byte[] taskBinaryBytes = null;
            if (stage instanceof ShuffleMapStage) {
                ShuffleMapStage mapStage = (ShuffleMapStage) stage;
                taskBinaryBytes = bufferToArray(closureSerializer.serialize(new Tuple2<>(stage.rdd,
                                                                                         mapStage.getShuffleDep())));
            } else if (stage instanceof ResultStage) {
                ResultStage resultStage = (ResultStage) stage;
                taskBinaryBytes = bufferToArray(closureSerializer.serialize(new Tuple2<>(stage.rdd,
                                                                                         resultStage.getJobAction())));
            }
            // TODO: 待实现broadcast
            taskBinary = sc.broadcast(taskBinaryBytes);
        } catch (NotSerializableException e) {
            abortStage(stage, "Task not serializable: " + e.toString(), e);
            runningStages.remove(stage);
            return;
        } catch (Throwable e) {
            abortStage(stage, "Task serialization failed: " + e + "\n" + exceptionString(e), e);
            runningStages.remove(stage);
            return;
        }

        // 每个分区对应一个Task
        Task<?>[] tasks = new Task[partitionsToCompute.size()];
        try {
            // TODO: TaskMetric
            if (stage instanceof ShuffleMapStage) {
                ShuffleMapStage mapStage = (ShuffleMapStage) stage;
                mapStage.clearWaitPartitionTask();
                for (int partition : partitionsToCompute) {
                    TaskLocation[] locs = taskIdToLocations.get(partition);
                    Partition part = stage.rdd.partitions()[partition];
                    mapStage.addWaitPartitionTask(partition);
                    tasks[partition] = new ShuffleMapTask(stage.id,
                                                          stage.latestInfo().attemptId,
                                                          taskBinary,
                                                          part,
                                                         locs,
                                                         properties,
                                                         jobId,
                                                         sc.applicationId(),
                                                         sc.applicationAttemptId());
                }
            } else if (stage instanceof ResultStage) {
                ResultStage resultStage = (ResultStage) stage;
                for (int partition : partitionsToCompute) {
                    int p = resultStage.partitions.get(partition);
                    Partition part = stage.rdd.partitions()[p];
                    TaskLocation[] locs = taskIdToLocations.get(partition);
                    tasks[partition] = new ResultTask<>(stage.id,
                                                        stage.latestInfo().attemptId,
                                                        taskBinary,
                                                        part,
                                                        locs,
                                                        partition,
                                                        properties,
                                                        jobId,
                                                        sc.applicationId(),
                                                        sc.applicationAttemptId());
                }
            }
        } catch (Throwable e) {
            abortStage(stage, "Task creation failed: " + e + "\n" + exceptionString(e), e);
            runningStages.remove(stage);
            return;
        }

        if (tasks.length > 0) {
            taskScheduler.submitTasks(new TaskSet(tasks,
                                                  stage.id,
                                                  stage.latestInfo().attemptId,
                                                  jobId, properties));
        } else {
            // Because we posted SparkListenerStageSubmitted earlier, we should mark
            // the stage as completed here in case there are no tasks to run
            markStageAsFinished(stage, "");

            String debugString = "";
            if (stage instanceof ShuffleMapStage) {
                debugString = String.format("Stage %s is actually done; (available: %s, available outputs: %s, " +
                                            "partitions: %d", stage, ((ShuffleMapStage) stage).isAvailable(),
                                            stage.numPartitions);
            } else if (stage instanceof ResultStage) {
                debugString = String.format("Stage %s is actually done; (partitions: %d", stage, stage.numPartitions);
            }
            LOGGER.debug(debugString);

            submitWaitingChildStages(stage);
        }
    }

    private void submitWaitingChildStages(Stage parent) {
        LOGGER.trace("Checking if any dependencies of {} are now runnable", parent);
        LOGGER.trace("running: {}", runningStages);
        LOGGER.trace("waiting: {}", waitingStages);
        LOGGER.trace("failed: {}", failedStages);
        List<Stage> childStages = waitingStages.stream()
                                               .filter(stage -> stage.parents.contains(parent))
                                               .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(childStages)) {
            childStages.forEach(waitingStages::remove);
            // TODO: 排序
            Collections.sort(childStages, (s1, s2) -> s1.firstJobId - s2.firstJobId);
            childStages.forEach(this::submitStage);
        }
    }

    private TaskLocation[] getPreferredLocs(RDD<?> rdd, int partition) {
        return getPreferredLocsInternal(rdd, partition, new HashSet<>());
    }

    private TaskLocation[] getPreferredLocsInternal(RDD<?> rdd,
                                                        int partition,
                                                        HashSet<Tuple2<RDD<?>, Integer>> visited) {
        // If the partition has already been visited, no need to re-visit.
        if (!visited.add(new Tuple2<>(rdd, partition))) {
            return new TaskLocation[0];
        }

        // If the partition is cached, return the cache locations
        TaskLocation[] taskLocation = getCacheLocs(rdd)[partition];
        if (taskLocation != null && taskLocation.length > 0) {
            return taskLocation;
        }

        // If the RDD has some placement preferences (as is the case for input RDDs), get those
        String[] rddPrefs = rdd.preferredLocations(rdd.partitions()[partition]);
        if (rddPrefs != null && rddPrefs.length > 0) {
            TaskLocation[] locations = new TaskLocation[rddPrefs.length];
            for (int i = 0; i < rddPrefs.length; ++i) {
                locations[i] = TaskLocation.apply(rddPrefs[i]);
            }
            return locations;
        }

        // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
        // that has any placement preferences. Ideally we would choose based on transfer sizes,
        // but this will do for now.
        List<Dependency<?>> dependencies = rdd.dependencies();
        if (dependencies == null) {
            return new TaskLocation[0];
        }
        for (Dependency<?> dep : dependencies) {
            if (dep instanceof NarrowDependency) {
                int[] parents = ((NarrowDependency) dep).getParents(partition);
                for (int inPart : parents) {
                    TaskLocation[] locs = getPreferredLocsInternal(dep.rdd(), inPart, visited);
                    if (locs != null) {
                        return locs;
                    }
                }
            }
        }
        return new TaskLocation[0];
    }


    private TaskLocation[][] getCacheLocs(RDD<?> rdd) {
        synchronized (cacheLocs) {
            if (!cacheLocs.containsKey(rdd.id)) {
                // Note: if the storage level is NONE, we don't need to get locations from block manager.
                TaskLocation[][] locs = null;
                if (rdd.storageLevel == StorageLevel.NONE) {
                    locs = new TaskLocation[rdd.partitions().length][];
                    for (int i = 0; i < locs.length; ++i) {
                        locs[i] = null;
                    }
                } else {
                    // 一个分区对应一个BlockId
                    BlockId[] blockIds = new BlockId[rdd.partitions().length];
                    for (int i = 0; i < rdd.partitions().length; ++i) {
                        blockIds[i] = new RDDBlockId(rdd.id, i);
                    }
                    BlockManagerId[][] blockManagerIds = blockManagerMaster.getLocations(blockIds);
                    locs = new TaskLocation[blockManagerIds.length][];
                    for (int partition = 0; partition < blockManagerIds.length; ++partition) {
                        // TODO: 检查整套实现流程
                        TaskLocation[] locations = new TaskLocation[blockManagerIds[partition].length];
                        for (int task = 0; task < blockManagerIds[partition].length; ++task) {
                            locations[task] = TaskLocation.apply(blockManagerIds[partition][task].host,
                                                                 blockManagerIds[partition][task].executorId);
                        }
                        locs[partition] = locations;
                    }
                    
                }
                cacheLocs.put(rdd.id, locs);
            }
            return cacheLocs.get(rdd.id);
        }
    }

    private List<Stage> getMissingParentStages(Stage stage) {
        Set<Stage> missing = Sets.newHashSet();
        Set<RDD<?>> visited = Sets.newHashSet();
        // We are manually maintaining a stack here to prevent StackOverflowError
        // caused by recursively visiting
        Stack<RDD<?>> waitingForVisit = new Stack<>();
        waitingForVisit.push(stage.rdd);
        while (waitingForVisit.size() > 0) {
            RDD<?> rdd = waitingForVisit.pop();
            if (!visited.contains(rdd)) {
                visited.add(rdd);
                boolean rddHasUncachedPartitions = getCacheLocs(rdd).length == 0;
                if (rddHasUncachedPartitions) {
                    List<Dependency<?>> dependencies = rdd.dependencies();
                    if (CollectionUtils.isNotEmpty(dependencies)) {
                        for (Dependency<?> dep : dependencies) {
                            if (dep instanceof ShuffleDependency) {
                                ShuffleDependency<?, ?, ?> shuffleDep = (ShuffleDependency<?, ?, ?>) dep;
                                ShuffleMapStage mapStage = getOrCreateShuffleMapStage(shuffleDep, stage.firstJobId);
                                if (!mapStage.isAvailable()) {
                                    missing.add(mapStage);
                                }
                            } else if (dep instanceof NarrowDependency) {
                                waitingForVisit.push(dep.rdd());
                            }
                        }
                    }
                }
            }

        }
        return Lists.newLinkedList(missing);
    }


    private int activeJobForStage(Stage stage) {
        List<Integer> jobsThatUseStage= Lists.newArrayList(stage.jobIds());
        Collections.sort(jobsThatUseStage);
        for (int jobId : jobsThatUseStage) {
            if (jobIdToActiveJob.containsKey(jobId)) {
                return jobId;
            }
        }
        return -1;
    }

    /**Create a ResultStage associated with the provided jobId*/
    private ResultStage createResultStage(RDD<?> rdd,
                                          JobAction<?, ?> jobAction,
                                          List<Integer> partitions,
                                          int jobId) {
        // step1: 计算final rdd依赖的所有ShuffleDependency
        // step2: 计算ShuffleDependency依赖的ShuffleMapStage
        // 调用链:
        //    DAGScheduler.getOrCreateParentStages(rdd)[final rdd ==> ResultStage]
        //      |
        //      +---> DAGScheduler.getShuffleDependencies(rdd)[final rdd ==> Set<ShuffleDependency>]
        //              |
        //              +---> DAGScheduler.getOrCreateShuffleMapStage(shuffleId, jobId)[ShuffleDependency ==> List<ShuffleMapStage>]
        //                     |
        //                     +---> DAGScheduler.createShuffleMapStage(shuffleDep, jobId)[ShuffleDependency ==> ShuffleMapStage]
        List<Stage> parents = getOrCreateParentStages(rdd, jobId);
        int id = nextStageId.getAndIncrement();
        ResultStage stage = new ResultStage(id,
                                            rdd,
                                            jobAction,
                                            partitions,
                                            parents,
                                            jobId);
        stageIdToStage.put(id, stage);
        updateJobIdStageIdMaps(jobId, Lists.newArrayList(stage));
        return stage;
    }

    private List<Stage> getOrCreateParentStages(RDD<?> rdd, int firstJobId) {
        Set<ShuffleDependency<?, ?, ?>> shuffleDependencies = getShuffleDependencies(rdd);
        if (CollectionUtils.isEmpty(shuffleDependencies)) {
            return Collections.emptyList();
        }
        // 根据ShuffleDependency创建Stage
        return shuffleDependencies.stream()
                                  .map(shuffleDep -> getOrCreateShuffleMapStage(shuffleDep, firstJobId))
                                  .collect(Collectors.toList());
    }

    /** Returns shuffle dependencies that are immediate parents of the given RDD*/
    private Set<ShuffleDependency<?, ?, ?>> getShuffleDependencies(RDD<?> rdd) {
        Set<ShuffleDependency<?, ?, ?>> parents = Sets.newHashSet();
        // 已遍历RDD
        Set<RDD<?>> visited = Sets.newHashSet();
        // 深度优先遍历
        Stack<RDD<?>> waitingForVisit = new Stack<>();
        waitingForVisit.push(rdd);

        while (waitingForVisit.size() > 0) {
            RDD<?> toVisit = waitingForVisit.pop();
            if (!visited.contains(toVisit)) {
                visited.add(toVisit);
                // 递归调用结束条件: RDD.dependencies() == null, 说明为DAG图入口
                if (toVisit.dependencies() != null) {
                    for (Dependency<?> dep : toVisit.dependencies()) {
                        if (dep instanceof ShuffleDependency) {
                            parents.add((ShuffleDependency<?, ?, ?>) dep);
                        } else {
                            // dep依赖的RDD入栈, 深度遍历
                            waitingForVisit.add(dep.rdd());
                        }
                    }
                }
            }
        }
        return parents;
    }

    /**
     * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
     * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
     * addition to any missing ancestor shuffle map stages.
     * */
    private ShuffleMapStage getOrCreateShuffleMapStage(ShuffleDependency<?, ?, ?> shuffleDep, int firstJobId) {
        ShuffleMapStage stage = shuffleIdToMapStage.get(shuffleDep.shuffleId());
        if (stage == null) {
            // step1: 计算ShuffleDependency依赖RDD的所有ShuffleDependency
            // step2: 生成ShuffleDependency对应ShuffleMapStage
            getMissingAncestorShuffleDependencies(shuffleDep.rdd()).forEach(dep -> {
                if (!shuffleIdToMapStage.containsKey(dep.shuffleId())) {
                    createShuffleMapStage(dep, firstJobId);
                }
            });
            return createShuffleMapStage(shuffleDep, firstJobId);
        }
        return stage;
    }

    /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
    private Stack<ShuffleDependency<?, ?, ?>> getMissingAncestorShuffleDependencies(RDD<?> rdd) {
        Stack<ShuffleDependency<?, ?, ?>> ancestors = new Stack<>();
        Set<RDD<?>> visited = Sets.newHashSet();
        // We are manually maintaining a stack here to prevent StackOverflowError
        // caused by recursively visiting
        Stack<RDD<?>> waitingForVisit = new Stack<>();
        waitingForVisit.push(rdd);
        while (waitingForVisit.size() > 0) {
            RDD<?> toVisit = waitingForVisit.pop();
            if (!visited.contains(toVisit)) {
                visited.add(toVisit);
                Set<ShuffleDependency<?, ?, ?>> shuffleDependencies = getShuffleDependencies(toVisit);
                if (shuffleDependencies == null || shuffleDependencies.size() == 0) {
                    continue;
                }
                shuffleDependencies.forEach(shuffleDep -> {
                    if (!shuffleIdToMapStage.containsKey(shuffleDep.shuffleId())) {
                        ancestors.push(shuffleDep);
                        waitingForVisit.push(shuffleDep.rdd());
                    }
                });
            }
        }
        return ancestors;
    }

    private ShuffleMapStage createShuffleMapStage(ShuffleDependency<?, ?, ?> shuffleDep, int jobId) {
        // step1: 计算ShuffleDependency依赖RDD的所有的ShuffleMapStage[同final RDD生成ResultStage, 递归调用]
        RDD<?> rdd = shuffleDep.rdd();
        int numTasks = rdd.partitions().length;
        List<Stage> parents = getOrCreateParentStages(rdd, jobId);
        int id = nextStageId.getAndIncrement();
        ShuffleMapStage stage = new ShuffleMapStage(id,
                                                    rdd,
                                                    numTasks,
                                                    parents,
                                                    jobId,
                                                    shuffleDep,
                                                    mapOutputTracker);

        // step2: 更新ShuffleId与ShuffleMapStage映射关系
        stageIdToStage.put(id, stage);
        shuffleIdToMapStage.put(shuffleDep.shuffleId(), stage);
        updateJobIdStageIdMaps(jobId, Lists.newArrayList(stage));

        // step3: MapOutputTracker记录ShuffleId对应依赖RDD输出分区数
        if (!mapOutputTracker.containsShuffle(shuffleDep.shuffleId())) {
            // Kind of ugly: need to register RDDs with the cache and map combiner tracker here
            // since we can't do it in the RDD constructor because # of partitions is unknown
            LOGGER.info("Registering RDD {}({})", rdd.id, rdd.creationSite.shortForm);
            mapOutputTracker.registerShuffle(shuffleDep.shuffleId(), rdd.partitions().length);
        }
        return stage;
    }

    private void updateJobIdStageIdMaps(int jobId,
                                        List<Stage> stages) {
        if (CollectionUtils.isNotEmpty(stages)) {
            Stage s = stages.get(0);
            s.jobIds().add(jobId);
            Set<Integer> stageIds = jobIdToStageIds.get(jobId);
            if (stageIds == null) {
                stageIds = Sets.newHashSet();
                jobIdToStageIds.put(jobId, stageIds);
            }
            stageIds.add(s.id);
            List<Stage> parentsWithoutThisJobId = s.parents.stream()
                                                           .filter(ps -> !ps.jobIds().contains(jobId))
                                                           .collect(Collectors.toList());
            updateJobIdStageIdMaps(jobId, parentsWithoutThisJobId);
        }
    }

    public void cancelJob(int jobId, String reason) {
        LOGGER.info("Asked to cancel job {}", jobId);
        eventProcessLoop.post(new JobCancelled(jobId, reason));
    }

    private void handleJobCancellation(int jobId, String reason) {
        if (jobIdToStageIds.containsKey(jobId)) {
            failJobAndIndependentStages(jobIdToActiveJob.get(jobId),
                                        String.format("Job %d cancelled %s", jobId, reason == null ? "" : reason));
        } else {
            LOGGER.debug("Trying to cancel unregistered job {}", jobId);
        }
    }

    /**Fails a job and all stages that are only used by that job, and cleans up relevant state*/
    private void failJobAndIndependentStages(ActiveJob activeJob, String reason) {
        failJobAndIndependentStages(activeJob, reason, null);
    }

    private void failJobAndIndependentStages(ActiveJob activeJob, String reason, Throwable e) {
        SparkException error = new SparkException(reason, e);
        boolean ableToCancelStages = true;

        // 是否中断作业线程
        boolean shouldInterruptThread = false;
        if (activeJob.properties != null) {
            shouldInterruptThread = toBoolean(activeJob.properties.getProperty(SPARK_JOB_INTERRUPT_ON_CANCEL, "false"));
        }

        // 当前Job依赖的所有Stage
        Set<Integer> stages = jobIdToStageIds.get(activeJob.jobId());
        if (stages == null || stages.isEmpty()) {
            LOGGER.error("No stages registered for job {}", activeJob.jobId());
            return;
        }

        // 取消依赖Stage的Task运行
        for (int stageId : stages) {
            Stage dependStage = stageIdToStage.get(stageId);
            if (dependStage == null ||
                    dependStage.jobIds() == null || !dependStage.jobIds().contains(activeJob.jobId())) {
                // 依赖的Stage并不包含JobID
                LOGGER.error("Job {} not registered for stage {} even though that stage was registered for the job",
                             activeJob.jobId(), stageId);
            } else if (dependStage.jobIds().size() == 1) {
                if (!stageIdToStage.containsKey(stageId)) { // TODO: 多余?
                    LOGGER.error("Missing Stage for stage with id {}", stageId);
                } else {
                    if (runningStages.contains(dependStage)) {
                        try {
                            taskScheduler.cancelTasks(stageId, shouldInterruptThread);
                            markStageAsFinished(dependStage, reason);
                        } catch (UnsupportedOperationException ex) {
                            LOGGER.info("Could not cancel tasks for stage {}", stageId, e);
                            ableToCancelStages = false;
                        }
                    }
                }
            }
        }

        if (ableToCancelStages) {
            // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
            // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
            cleanupStateForJobAndIndependentStages(activeJob);
            activeJob.listener().jobFailed(error);
            listenerBus.post(new SparkListenerJobEnd(activeJob.jobId(),
                                                     clock.getTimeMillis(),
                                                     new JobFailed(error)));
        }
    }

    private void cleanupStateForJobAndIndependentStages(ActiveJob job) {
        Set<Integer> registeredStages = jobIdToStageIds.get(job.jobId());
        if (registeredStages == null || registeredStages.isEmpty()) {
            LOGGER.error("No stages registered for job {}", job.jobId());
        } else {
            Iterator<Integer> iterator = stageIdToStage.keySet().iterator();
            while (iterator.hasNext()) {
                int stageId = iterator.next();
                Stage stage = stageIdToStage.get(stageId);
                if (!registeredStages.contains(stageId)) {
                    continue;
                }
                Set<Integer> jobSet = stage.jobIds();
                if (jobSet == null || !jobSet.contains(job.jobId())) {
                    LOGGER.error("Job {} not registered for stage {} even though that stage was registered for the job",
                            job.jobId(), stageId);
                    continue;
                }
                jobSet.remove(job.jobId());
                if (jobSet.isEmpty()) {
                   // 删除Stage
                    if (runningStages.contains(stage)) {
                        LOGGER.debug("Removing running stage {}", stageId);
                        runningStages.remove(stage);
                    }
                    ShuffleMapStage mapStage = shuffleIdToMapStage.get(stageId);
                    if (mapStage != null) {
                        shuffleIdToMapStage.remove(stageId);
                    }
                    if (waitingStages.contains(stage)) {
                        LOGGER.debug("Removing stage {} from waiting set.", stageId);
                        waitingStages.remove(stage);
                    }
                    if (failedStages.contains(stage)) {
                        LOGGER.debug("Removing stage %d from failed set.", stageId);
                        failedStages.remove(stage);
                    }
                    iterator.remove();
                    // data structures based on StageId
                    LOGGER.debug("After removal of stage {}, remaining stages = {}", stageId, stageIdToStage.size());
                }
            }
        }
        jobIdToStageIds.remove(job.jobId());
        jobIdToActiveJob.remove(job.jobId());
        activeJobs.remove(job);
        if (job.finalStage() != null) {
            if (job.finalStage() instanceof ResultStage) {
                ResultStage resultStage = (ResultStage) job.finalStage();
                resultStage.removeActiveJob();
            } else if (job.finalStage() instanceof ShuffleMapStage) {
                ShuffleMapStage shuffleMapStage = (ShuffleMapStage) job.finalStage();
                shuffleMapStage.removeActiveJob(job);
            }
        }
    }

    private void markMapStageJobAsFinished(ActiveJob job, MapOutputStatistics stats) {
        // In map stage jobs, we only create a single "task", which is to finish all of the stage
        // (including reusing any previous map outputs, etc); so we just mark task 0 as done
        job.markPartitionTaskFinished(0);
        job.listener().taskSucceeded(0, stats);
        cleanupStateForJobAndIndependentStages(job);
        listenerBus.post(new SparkListenerJobEnd(job.jobId(), clock.getTimeMillis(), new JobResult.JobSucceeded()));
    }

    private void markMapStageJobsAsFinished(ShuffleMapStage shuffleStage) {
        // Mark any map-stage jobs waiting on this stage as finished
        if (shuffleStage.isAvailable() && shuffleStage.mapStageJobs.size() > 0) {
            MapOutputStatistics stats = mapOutputTracker.getStatistics(shuffleStage.getShuffleDep());
            for (ActiveJob job : shuffleStage.mapStageJobs) {
                markMapStageJobAsFinished(job, stats);
            }
        }
    }

    private void markStageAsFinished(Stage stage, String errorMessage) {
        String serviceTime = "Unknown";
        if (stage.latestInfo().submissionTime() != -1) {
            serviceTime = String.format("%.03f", (clock.getTimeMillis() - stage.latestInfo().submissionTime()) / 1000.0);
        }

        if (isNotEmpty(errorMessage)) {
            LOGGER.info("{} ({}) finished in {} s", stage, stage.name, serviceTime);
            stage.latestInfo().setCompletionTime(clock.getTimeMillis());

            // Clear failure count for this stage, now that it's succeeded.
            // We only limit consecutive failures of stage attempts,so that if a stage is
            // re-used many times in a long-running job, unrelated failures don't eventually cause the
            // stage to be aborted.
            stage.clearFailures();
        } else {
            stage.latestInfo().stageFailed(errorMessage);
            LOGGER.info("{} ({}) failed in {} s due to {}", stage, stage.name, serviceTime, errorMessage);
        }

        outputCommitCoordinator.stageEnd(stage.id);
        listenerBus.post(new SparkListenerStageCompleted(stage.latestInfo()));
        runningStages.remove(stage);
    }

    private void handleExecutorLost(String execId, boolean workerLost) {
        // if the cluster manager explicitly tells us that the entire worker was lost, then
        // we know to unregister shuffle output.  (Note that "worker" specifically refers to the process
        // from a Standalone cluster, where the shuffle service lives in the Worker.)
        boolean fileLost = workerLost || !env.blockManager.externalShuffleServiceEnabled;
        removeExecutorAndUnregisterOutputs(execId,
                                           fileLost,
                                           null,
                                           -1);
    }

    private void removeExecutorAndUnregisterOutputs(String execId,
                                                    boolean fileLost,
                                                    String hostToUnregisterOutputs,
                                                    long maybeEpoch) {
        long currentEpoch = maybeEpoch;
        if (maybeEpoch == -1) {
            currentEpoch = mapOutputTracker.getEpoch();
        }
        if (!failedEpoch.containsKey(execId) || failedEpoch.get(execId) < currentEpoch) {
            failedEpoch.put(execId, currentEpoch);
            LOGGER.info("Executor lost: {} (epoch {})", execId, currentEpoch);
            blockManagerMaster.removeExecutor(execId);
            if (fileLost) {
                if (isNotEmpty(hostToUnregisterOutputs)) {
                    LOGGER.info("Shuffle files lost for host: {} (epoch {})", hostToUnregisterOutputs, currentEpoch);
                    mapOutputTracker.removeOutputsOnHost(hostToUnregisterOutputs);
                } else {
                    LOGGER.info("Shuffle files lost for executor: {} (epoch {})", execId, currentEpoch);
                    mapOutputTracker.removeOutputsOnExecutor(execId);
                }
                clearCacheLocs();
            } else {
                LOGGER.debug("Additional executor lost message for {} (epoch {})", execId, currentEpoch);
            }
        }
    }

    /**
     * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
     * cancellation of the job itself.
     * */
    public void taskSetFailed(TaskSet taskSet, String reason, Throwable exception) {
        eventProcessLoop.post(new TaskSetFailed(taskSet, reason, exception));
    }

    private void handleTaskSetFailed(TaskSet taskSet, String reason, Throwable exception) {
        Stage abortStage = stageIdToStage.get(taskSet.stageId);
        if (abortStage != null) {
            abortStage(abortStage, reason, exception);
        }
    }

    /**
     * Called by the TaskSetManager to report that a task has completed
     * and results are being fetched remotely.
     * */
    public void taskGettingResult(TaskInfo taskInfo) {
        eventProcessLoop.post(new GettingResultEvent(taskInfo));
    }

    private void handleGetTaskResult(TaskInfo taskInfo) {
        listenerBus.post(new SparkListenerTaskGettingResult(taskInfo));
    }

    private void postTaskEnd(CompletionEvent event) {
        // TODO: Task Metric
        Task<?> task = event.getTask();
        listenerBus.post(new SparkListenerTaskEnd(task.stageId, task.stageAttemptId, getFormattedClassName(task), event.getReason(), event.getTaskInfo()));
    }

    private void handleTaskCompletion(CompletionEvent event) {
        Task<?> task = event.getTask();
        outputCommitCoordinator.taskCompleted(
                task.stageId,
                task.stageAttemptId,
                task.partitionId,
                event.getTaskInfo().attemptNumber,
                event.getReason());

        if (!stageIdToStage.containsKey(task.stageId)) {
            postTaskEnd(event);
            return;
        }

        Stage stage = stageIdToStage.get(task.stageId);

        // Make sure the task's accumulators are updated before any other processing happens, so that
        // we can post a task end event before any jobs or stages are updated. The accumulators are
        // only updated in certain cases.
        if (event.getReason() instanceof Success) {
            // TODO: update task metric
        } else if (event.getReason() instanceof ExceptionFailure) {
            // TODO: update task metric
        }

        postTaskEnd(event);

        TaskEndReason reason = event.getReason();
        if (reason instanceof Success) {
            if (task instanceof ResultTask) {
                ResultTask rt = (ResultTask) task;
                ResultStage resultStage = (ResultStage) stage;
                ActiveJob job = resultStage.getActiveJob();
                if (job == null) {
                    LOGGER.info("Ignoring result from {} because its job has finished", task);
                } else {
                    if (!job.isPartitionTaskFinished(rt.getOutputId())) {
                        job.markPartitionTaskFinished(rt.getOutputId());
                        if (job.isJobFinished()) {
                            markStageAsFinished(stage, null);
                            cleanupStateForJobAndIndependentStages(job);
                            listenerBus.post(new SparkListenerJobEnd(job.jobId(), System.currentTimeMillis(), new JobResult.JobSucceeded()));
                        }
                    }
                    try {
                        job.listener().taskSucceeded(rt.getOutputId(), event.getResult());
                    } catch (Exception e) {
                        job.listener().jobFailed(new SparkDriverExecutionException(e));
                    }
                }
            } else if (task instanceof ShuffleMapTask) {
                ShuffleMapTask smt = (ShuffleMapTask) task;
                ShuffleMapStage mapStage = (ShuffleMapStage) stage;
                MapStatus mapStatus = (MapStatus) event.getResult();
                String execId = mapStatus.location().executorId;
                LOGGER.debug("ShuffleMapTask finished on {}", execId);
                if (stageIdToStage.get(task.stageId).latestInfo().attemptNumber() == task.stageAttemptId) {
                    // This task was for the currently running attempt of the stage. Since the task
                    // completed successfully from the perspective of the TaskSetManager, mark it as
                    // no longer pending (the TaskSetManager may consider the task complete even
                    // when the output needs to be ignored because the task's epoch is too small below.
                    // In this case, when pending partitions is empty, there will still be missing
                    // output locations, which will cause the DAGScheduler to resubmit the stage below.)
                    mapStage.markPartitionTaskFinished(task.partitionId);
                }
                if (failedEpoch.containsKey(execId) && smt.epoch <= failedEpoch.get(execId)) {
                    LOGGER.info("Ignoring possibly bogus {} completion from executor {}", smt, execId);
                } else {
                    // The epoch of the task is acceptable (i.e., the task was launched after the most
                    // recent failure we're aware of for the executor), so mark the task's output as
                    // available.
                    // 注册MapTask的结果输出地址信息
                    mapOutputTracker.registerMapOutput(mapStage.getShuffleDep().shuffleId(),
                            smt.partitionId, mapStatus);
                    // Remove the task's partition from pending partitions. This may have already been
                    // done above, but will not have been done yet in cases where the task attempt was
                    // from an earlier attempt of the stage (i.e., not the attempt that's currently
                    // running).  This allows the DAGScheduler to mark the stage as complete when one
                    // copy of each task has finished successfully, even if the currently active stage
                    // still has tasks running.
                    mapStage.markPartitionTaskFinished(task.partitionId);
                }

                if (runningStages.contains(stage) && mapStage.isWaitPartitionTaskFinished()) {
                    markStageAsFinished(stage, null);
                    LOGGER.info("looking for newly runnable stages");
                    LOGGER.info("running: {}", runningStages);
                    LOGGER.info("waiting: {}", waitingStages);
                    LOGGER.info("failed: {}", failedStages);

                    // This call to increment the epoch may not be strictly necessary, but it is retained
                    // for now in order to minimize the changes in behavior from an earlier version of the
                    // code. This existing behavior of always incrementing the epoch following any
                    // successful shuffle map stage completion may have benefits by causing unneeded
                    // cached map outputs to be cleaned up earlier on executors. In the future we can
                    // consider removing this call, but this will require some extra investigation.
                    // See https://github.com/apache/spark/pull/17955/files#r117385673 for more details.
                    mapOutputTracker.incrementEpoch();

                    clearCacheLocs();

                    // 某个PartitionTask执行失败, 则重新运行Task
                    if (!mapStage.isAvailable()) {
                        // Some tasks had failed; let's resubmit this shuffleStage.
                        // TODO: Lower-level scheduler should also deal with this
                        LOGGER.info("Resubmitting {}({}) because some of its tasks had failed: {}",
                                mapStage, mapStage.name, join(mapStage.findMissingPartitions(), ","));
                        submitStage(mapStage);
                    } else {
                        markMapStageJobsAsFinished(mapStage);
                        submitWaitingChildStages(stage);
                    }
                }
            }
        } else if (reason instanceof Resubmitted) {

        } else if (reason instanceof FetchFailed) {

        } else if (reason instanceof TaskCommitDenied) {

        } else if (reason instanceof ExceptionFailure) {

        } else if (reason instanceof TaskResultLost) {

        } else if (reason instanceof ExecutorLostFailure || reason instanceof TaskKilled || reason instanceof UnknownReason) {

        }
    }

    private class DAGSchedulerEventProcessLoop extends EventLoop<DAGSchedulerEvent> {
        private final Logger LOGGER = LoggerFactory.getLogger(DAGSchedulerEventProcessLoop.class);

        private DAGScheduler dagScheduler;

        DAGSchedulerEventProcessLoop(DAGScheduler dagScheduler) {
            super("dag-scheduler-event-loop");
            this.dagScheduler = dagScheduler;
        }

        @Override
        public void onReceive(DAGSchedulerEvent event) {
            long start = System.currentTimeMillis();
            try {
                doOnReceive(event);
            } finally {
                LOGGER.info("DAGEvent[{}]处理耗时: {}ms", event.getClass().getSimpleName(), System.currentTimeMillis() - start);
            }
        }

        @Override
        public void onError(Throwable cause) {

        }

        private void doOnReceive(DAGSchedulerEvent event) {
            if (event instanceof JobSubmitted) {
                // Job生成
                JobSubmitted jobSubmitted = (JobSubmitted) event;
                dagScheduler.handleJobSubmitted(jobSubmitted.jobId,
                                                jobSubmitted.finalRDD,
                                                jobSubmitted.jobAction,
                                                jobSubmitted.partitions,
                                                jobSubmitted.listener,
                                                jobSubmitted.properties);
            } else if (event instanceof JobCancelled) {
                // Job取消
                JobCancelled jobCancelled = (JobCancelled) event;
                dagScheduler.handleJobCancellation(jobCancelled.jobId,
                                                   jobCancelled.reason);
            } else if (event instanceof ExecutorLost) {
                ExecutorLost lost = (ExecutorLost) event;
                boolean workerLost = false;
                if (lost.reason instanceof SlaveLost) {
                    workerLost = true;
                }
                dagScheduler.handleExecutorLost(lost.execId, workerLost);
            } else if (event instanceof TaskSetFailed) {
                TaskSetFailed taskSetFailed = (TaskSetFailed) event;
                dagScheduler.handleTaskSetFailed(taskSetFailed.taskSet,
                                                 taskSetFailed.reason,
                                                 taskSetFailed.exception);
            } else if (event instanceof GettingResultEvent) {
                GettingResultEvent gettingResultEvent = (GettingResultEvent) event;
                dagScheduler.handleGetTaskResult(gettingResultEvent.taskInfo);
            } else if (event instanceof CompletionEvent) {
                CompletionEvent completionEvent = (CompletionEvent) event;
                dagScheduler.handleTaskCompletion(completionEvent);
            }
        }
    }
}
