package com.sdu.spark.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.*;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.scheduler.DAGSchedulerEvent.JobCancelled;
import com.sdu.spark.scheduler.DAGSchedulerEvent.JobSubmitted;
import com.sdu.spark.scheduler.JobResult.JobFailed;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerJobEnd;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerJobStart;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerStageCompleted;
import com.sdu.spark.scheduler.action.RDDAction;
import com.sdu.spark.scheduler.action.ResultHandler;
import com.sdu.spark.storage.BlockManagerMaster;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.CallSite;
import com.sdu.spark.utils.Clock;
import com.sdu.spark.utils.Clock.SystemClock;
import com.sdu.spark.utils.EventLoop;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.sdu.spark.SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL;
import static org.apache.commons.lang3.BooleanUtils.toBoolean;

/**
 * {@link DAGScheduler}职责:
 *
 * 1: {@link #runJob(RDD, RDDAction, List, CallSite, ResultHandler, Properties)}划分并提及Stage
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
 *    2': 划分Stage过程中需维护StageId与Stage、ShuffleId与ShuffleMapStage映射关系, 于此同时需要MapOutputTrackerMaster
 *
 *        维护ShuffleId与parent rdd的分区数映射关系
 *
 * 2: {@link #cancelJob(int, String)}终止作业及Job关联的Stage信息
 *
 * @author hanhan.zhang
 * */
@SuppressWarnings("unchecked")
public class DAGScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DAGScheduler.class);

    private SparkContext sc;
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
    private final Map<Integer, List<TaskLocation>> cacheLocs = Maps.newHashMap();



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
        this.clock = clock;
        // 初始化
        this.eventProcessLoop = new DAGSchedulerEventProcessLoop(this);
        this.eventProcessLoop.start();
        this.taskScheduler.setDAGScheduler(this);
    }

    /********************************Spark DAG State调度*********************************/
    /**
     * @param rdd target RDD to run tasks on
     * @param rddAction a function to run on each partition of the RDD
     * @param partitions set of partitions to run on; some jobs may not want to compute on all
     *                   partitions of the target RDD, e.g. for operations like first()
     * @param callSite where in the user program this job was called
     * @param resultHandler callback to pass each result to
     * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
     * */
    public <T, U> void runJob(RDD<T> rdd,
                              RDDAction<T, U> rddAction,
                              List<Integer> partitions,
                              CallSite callSite,
                              ResultHandler<U> resultHandler,
                              Properties properties) throws Exception {
        long start = System.nanoTime();
        JobWaiter<U> waiter = submitJob(rdd, rddAction, partitions, callSite, resultHandler, properties);
        Future<Boolean> future = waiter.completionFuture();
        try {
            boolean success = future.get();
            if (success) {
                LOGGER.info("Job {} finished: {}, took {} s", waiter.jobId, callSite.shortForm,
                            (System.nanoTime() - start) / 1e9);
            }
        } catch (Exception e) {
            LOGGER.error("Job {} failed: {}, took {} s", waiter.jobId, callSite.shortForm,
                        (System.nanoTime() - start) / 1e9);
            // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
            StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
            StackTraceElement callerStackTrace = stackTraces[stackTraces.length - 1];
            StackTraceElement[] exceptionStackTrace = e.getStackTrace();
            e.setStackTrace(ArrayUtils.add(exceptionStackTrace, callerStackTrace));
            throw e;
        }
    }

    private <T, U> JobWaiter<U> submitJob(RDD<T> rdd,
                                          RDDAction<T, U> rddAction,
                                          List<Integer> partitions,
                                          CallSite callSite,
                                          ResultHandler<U> resultHandler,
                                          Properties properties) {
        // 校验传入RDD分区数是否正确
        int maxPartitions = rdd.partitions().size();
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
                                                 rddAction,
                                                 partitions,
                                                 callSite,
                                                 waiter,
                                                 new Properties(properties)));
        return waiter;
    }

    private <T, U> void handleJobSubmitted(int jobId,
                                           RDD<T> finalRDD,
                                           RDDAction<T, U> rddAction,
                                           List<Integer> partitions,
                                           CallSite callSite,
                                           JobListener listener,
                                           Properties properties) {
        ResultStage finalStage;
        try {
            // New stage creation may throw an exception if, for example, jobs are run on a
            // HadoopRDD whose underlying HDFS files have been deleted.
            finalStage = createResultStage(finalRDD, rddAction, partitions, jobId, callSite);
        } catch (Exception e){
            listener.jobFailed(e);
            return;
        }

        ActiveJob job = new ActiveJob(jobId, finalStage, callSite, listener, properties);
        clearCacheLocs();
        LOGGER.info("Got job {} ({}) with {} combiner partitions", job.jobId(),
                                                                   callSite.shortForm,
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

    private synchronized void clearCacheLocs() {
        cacheLocs.clear();
    }

    private void submitStage(Stage stage) {
        int jobId = activeJobForStage(stage);
        if (jobId != -1) {
            LOGGER.info("提交Spark Job Stage[stageId = {}, firstJobId = {}]", stage.id, stage.firstJobId);
            if (!waitingStages.contains(stage) && !runningStages.contains(stage)
                    && !failedStages.contains(stage)) {
                List<Stage> missing = getMissingParentStages(stage);

                if (CollectionUtils.isEmpty(missing)) {
                    submitMissingTasks(stage, jobId);
                } else {
                    Collections.sort(missing, (o1, o2) -> o1.id - o2.id);
                    missing.forEach(this::submitStage);
                    waitingStages.add(stage);
                }
            }
        } else {

        }
    }

    private void abortStage(Stage failedStage, String reason, Throwable exception) {
        if (!stageIdToStage.containsKey(failedStage.id)) {
            // Skip all the actions if the stage has been removed.
            return;
        }
//        List<ActiveJob> dependentJobs;
//        val dependentJobs: Seq[ActiveJob] =
//                activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
//        failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
//        for (job <- dependentJobs) {
//            failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
//        }
//        if (dependentJobs.isEmpty) {
//            logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
//        }
    }

    private void submitMissingTasks(Stage stage, int jobId) {

    }

    private List<TaskLocation> getCacheLocs(RDD<?> rdd) {
        synchronized (cacheLocs) {
            if (!cacheLocs.containsKey(rdd.id)) {
                // Note: if the storage level is NONE, we don't need to get locations from block manager.
                List<TaskLocation> locs = null;
                if (rdd.storageLevel == StorageLevel.NONE) {

                } else {

                }
                cacheLocs.put(rdd.id, locs);
            }
            return cacheLocs.get(rdd.id);
        }
    }

    private void visit(RDD<?> rdd,
                       Stage stage,
                       Set<RDD<?>> visited,
                       Set<Stage> missing,
                       Stack<RDD<?>> waitingForVisit) {
        if (!visited.contains(rdd)) {
            visited.add(rdd);
            boolean rddHasUncachedPartitions = getCacheLocs(rdd).isEmpty();
            if (rddHasUncachedPartitions) {
                for (Dependency dep : rdd.dependencies()) {
                    if (dep instanceof ShuffleDependency) {
                        ShuffleDependency<?, ?, ?> shuffleDep = (ShuffleDependency<?, ?, ?>) dep;
                        ShuffleMapStage mapStage = getOrCreateShuffleMapStage(shuffleDep, stage.firstJobId);
                        if (!mapStage.isAvailable()) {
                            missing.add(mapStage);
                        }
                    } else if (dep instanceof NarrowDependency) {
                        NarrowDependency<?> narrowDep = (NarrowDependency<?>) dep;
                        waitingForVisit.push(narrowDep.rdd());
                    }
                }
            }
        }
    }
    private List<Stage> getMissingParentStages(Stage stage) {
        Set<Stage> missing = Sets.newHashSet();
        Set<RDD<?>> visited = Sets.newHashSet();
        // We are manually maintaining a stack here to prevent StackOverflowError
        // caused by recursively visiting
        Stack<RDD<?>> waitingForVisit = new Stack<>();
        waitingForVisit.push(stage.rdd);
        while (CollectionUtils.isNotEmpty(waitingForVisit)) {
            visit(waitingForVisit.pop(), stage, visited, missing, waitingForVisit);
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
                                          RDDAction<?, ?> rddAction,
                                          List<Integer> partitions,
                                          int jobId,
                                          CallSite callSite) {
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
                                            rddAction,
                                            partitions,
                                            parents,
                                            jobId,
                                            callSite);
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
        int numTasks = rdd.partitions().size();
        List<Stage> parents = getOrCreateParentStages(rdd, jobId);
        int id = nextStageId.getAndIncrement();
        ShuffleMapStage stage = new ShuffleMapStage(id,
                                                    rdd,
                                                    numTasks,
                                                    parents,
                                                    jobId,
                                                    rdd.creationSite,
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
            mapOutputTracker.registerShuffle(shuffleDep.shuffleId(), rdd.partitions().size());
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

    private void markStageAsFinished(Stage stage, String errorMessage) {
        String serviceTime = "Unknown";
        if (stage.latestInfo().submissionTime() != -1) {
            serviceTime = String.format("%.03f", (clock.getTimeMillis() - stage.latestInfo().submissionTime()) / 1000.0);
        }

        if (StringUtils.isNotEmpty(errorMessage)) {
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

    private class DAGSchedulerEventProcessLoop extends EventLoop<DAGSchedulerEvent> {
        private final Logger LOGGER = LoggerFactory.getLogger(DAGSchedulerEventProcessLoop.class);

        private DAGScheduler dagScheduler;

        public DAGSchedulerEventProcessLoop(DAGScheduler dagScheduler) {
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
                                                jobSubmitted.rddAction,
                                                jobSubmitted.partitions,
                                                jobSubmitted.callSite,
                                                jobSubmitted.listener,
                                                jobSubmitted.properties);
            } else if (event instanceof JobCancelled) {
                // Job取消
                JobCancelled jobCancelled = (JobCancelled) event;
                dagScheduler.handleJobCancellation(jobCancelled.jobId,
                                                   jobCancelled.reason);
            }
        }
    }
}
