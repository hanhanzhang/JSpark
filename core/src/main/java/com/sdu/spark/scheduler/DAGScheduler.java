package com.sdu.spark.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.*;
import com.sdu.spark.scheduler.SparkListenerEvent.*;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.rdd.Transaction;
import com.sdu.spark.scheduler.DAGSchedulerEvent.JobSubmitted;
import com.sdu.spark.storage.BlockManagerMaster;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.CallSite;
import com.sdu.spark.utils.Clock;
import com.sdu.spark.utils.Clock.SystemClock;
import com.sdu.spark.utils.EventLoop;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.sdu.spark.utils.Utils.getFutureResult;

/**
 * @author hanhan.zhang
 * */
public class DAGScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DAGScheduler.class);

    private SparkContext sc;
    private TaskScheduler taskScheduler;
    private LiveListenerBus listenerBus;
    private MapOutputTrackerMaster mapOutputTracker;
    private BlockManagerMaster blockManagerMaster;
    private SparkEnv env;
    private Clock clock;

    private AtomicInteger nextJobId = new AtomicInteger(0);
    private Map<Integer, ShuffleMapStage> shuffleIdToMapStage = Maps.newHashMap();
    private AtomicInteger nextStageId = new AtomicInteger(0);
    private Map<Integer, Stage> stageIdToStage = Maps.newHashMap();
    private Set<Stage> waitingStages = Sets.newHashSet();
    private Set<Stage> runningStages = Sets.newHashSet();
    private Set<Stage> failedStages = Sets.newHashSet();
    private Map<Integer, Set<Integer>> jobIdToStageIds = Maps.newHashMap();
    private Map<Integer, ActiveJob> jobIdToActiveJob = Maps.newHashMap();
    private Set<ActiveJob> activeJobs = Sets.newHashSet();
    private Map<Integer, List<TaskLocation>> cacheLocs = Maps.newHashMap();





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

    public DAGScheduler(SparkContext sc, TaskScheduler taskScheduler, LiveListenerBus listenerBus,
                        MapOutputTrackerMaster mapOutputTracker, BlockManagerMaster blockManagerMaster,
                        SparkEnv env, Clock clock) {
        this.sc = sc;
        this.taskScheduler = taskScheduler;
        this.listenerBus = listenerBus;
        this.mapOutputTracker = mapOutputTracker;
        this.blockManagerMaster = blockManagerMaster;
        this.env = env;
        this.clock = clock;
        // 初始化
        this.eventProcessLoop = new DAGSchedulerEventProcessLoop(this);
        this.eventProcessLoop.start();
        this.taskScheduler.setDAGScheduler(this);
    }

    /********************************Spark DAG State调度*********************************/
    public <T, U> void runJob(RDD<T> rdd,
                              Transaction<Pair<TaskContext, Iterator<T>>, U> func,
                              List<Integer> partitions,
                              CallSite callSite,
                              Transaction<Pair<Integer, U>, Void> resultHandler,
                              Properties properties) {
        long start = System.currentTimeMillis();
        JobWaiter<U> waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties);
        Future<Boolean> future = waiter.completionFuture();
        boolean result = getFutureResult(future);
        if (result) {
            LOGGER.info("Spark Job[jobId = {}]完成: {}, 耗时{}ms",
                        waiter.jobId,
                        callSite,
                        System.currentTimeMillis() - start);
        } else {

        }
    }
    private <T, U> JobWaiter<U> submitJob(RDD<T> rdd,
                                          Transaction<Pair<TaskContext, Iterator<T>>, U> func,
                                          List<Integer> partitions,
                                          CallSite callSite,
                                          Transaction<Pair<Integer, U>, Void> resultHandler,
                                          Properties properties) {
        int maxPartitions = rdd.partitions().size();
        rdd.partitions().forEach(p -> {
            if (p.index() < 0 || p.index() > maxPartitions) {
                throw new IllegalArgumentException(String.format("Attempting to access a non-existent partition : %s, Total number of partitions: %s", p.index(), maxPartitions));
            }
        });

        int jobId = nextJobId.getAndIncrement();
        if (partitions.size() == 0) {
            return new JobWaiter<>(this, jobId, 0, resultHandler);
        }

        assert(partitions.size() > 0);
        JobWaiter<U> waiter = new JobWaiter<>(this, jobId, partitions.size(), resultHandler);
        eventProcessLoop.post(new JobSubmitted<>(jobId,
                                                 rdd,
                                                 func,
                                                 partitions,
                                                 callSite,
                                                 waiter,
                                                 new Properties(properties)));
        return waiter;
    }

    private <T, U> void handleJobSubmitted(int jobId,
                                           RDD<T> finalRDD,
                                           Transaction<Pair<TaskContext, Iterator<?>>, ?> func,
                                           List<Integer> partitions,
                                           CallSite callSite,
                                           JobListener listener,
                                           Properties properties) {
        ResultStage finalStage;
        try {
            // New stage creation may throw an exception if, for example, jobs are run on a
            // HadoopRDD whose underlying HDFS files have been deleted.
            finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite);
        } catch (Exception e){
            listener.jobFailed(e);
            return;
        }

        ActiveJob job = new ActiveJob(jobId, finalStage, callSite, listener, properties);
//        clearCacheLocs();
//        logInfo("Got job %s (%s) with %d output partitions".format(
//                job.jobId, callSite.shortForm, partitions.length))
//        logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
//        logInfo("Parents of final stage: " + finalStage.parents)
//        logInfo("Missing parents: " + getMissingParentStages(finalStage))

        long jobSubmissionTime = clock.getTimeMillis();
        jobIdToActiveJob.put(jobId, job);
        activeJobs.add(job);
        finalStage.setActiveJob(job);
        Set<Integer> stageIds = jobIdToStageIds.get(jobId);
        List<StageInfo> stageInfos = stageIds.stream()
                                             .map(id -> stageIdToStage.get(id).latestInfo)
                                             .collect(Collectors.toList());
        listenerBus.post(new SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties));
        submitStage(finalStage);
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

    private void visit(RDD<?> rdd, Stage stage, Set<RDD<?>> visited, Set<Stage> missing, Stack<RDD<?>> waitingForVisit) {
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
        List<Integer> jobsThatUseStage= Lists.newArrayList(stage.jobIds);
        Collections.sort(jobsThatUseStage);
        for (int jobId : jobsThatUseStage) {
            if (jobIdToActiveJob.containsKey(jobId)) {
                return jobId;
            }
        }
        return -1;
    }

    private ResultStage createResultStage(RDD<?> rdd,  Transaction<Pair<TaskContext, Iterator<?>>, ?> func,
                                          List<Integer> partitions, int jobId, CallSite callSite) {
        List<Stage> parents = getOrCreateParentStages(rdd, jobId);
        int id = nextStageId.getAndIncrement();
        ResultStage stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite);
        stageIdToStage.put(id, stage);
        updateJobIdStageIdMaps(jobId, Lists.newArrayList(stage));
        return stage;
    }

    private List<Stage> getOrCreateParentStages(RDD<?> rdd, int firstJobId) {
        return getShuffleDependencies(rdd).stream()
                                          .map(shuffleDep -> getOrCreateShuffleMapStage(shuffleDep, firstJobId))
                                          .collect(Collectors.toList());
    }
    private Set<ShuffleDependency<?, ?, ?>> getShuffleDependencies(RDD<?> rdd) {
        // 深度优先遍历
        Set<ShuffleDependency<?, ?, ?>> parents = Sets.newHashSet();
        Set<RDD<?>> visited = Sets.newHashSet();
        Stack<RDD<?>> waitingForVisit = new Stack<>();
        waitingForVisit.push(rdd);
        while (CollectionUtils.isNotEmpty(waitingForVisit)) {
            RDD<?> toVisit = waitingForVisit.pop();
            if (!visited.contains(toVisit)) {
                visited.add(toVisit);
                toVisit.dependencies().forEach(dep -> {
                    if (dep instanceof ShuffleDependency) {
                        parents.add((ShuffleDependency<?, ?, ?>) dep);
                    } else {
                        waitingForVisit.push(dep.rdd());
                    }
                });
            }
        }
        return parents;
    }

    private ShuffleMapStage getOrCreateShuffleMapStage(ShuffleDependency<?, ?, ?> shuffleDep, int firstJobId) {
        ShuffleMapStage stage = shuffleIdToMapStage.get(shuffleDep.shuffleId);
        if (stage == null) {
            getMissingAncestorShuffleDependencies(shuffleDep.rdd()).forEach(dep -> {
                if (!shuffleIdToMapStage.containsKey(dep.shuffleId)) {
                    createShuffleMapStage(dep, firstJobId);
                }
            });
            return createShuffleMapStage(shuffleDep, firstJobId);
        }
        return stage;
    }

    private Stack<ShuffleDependency<?, ?, ?>> getMissingAncestorShuffleDependencies(RDD<?> rdd) {
        Stack<ShuffleDependency<?, ?, ?>> ancestors = new Stack<>();
        Set<RDD<?>> visited = Sets.newHashSet();
        // We are manually maintaining a stack here to prevent StackOverflowError
        // caused by recursively visiting
        Stack<RDD<?>> waitingForVisit = new Stack<>();
        waitingForVisit.push(rdd);
        while (CollectionUtils.isNotEmpty(waitingForVisit)) {
            RDD<?> toVisit = waitingForVisit.pop();
            if (!visited.contains(toVisit)) {
                visited.add(toVisit);
                getShuffleDependencies(toVisit).forEach(shuffleDep -> {
                    if (!shuffleIdToMapStage.containsKey(shuffleDep.shuffleId)) {
                        ancestors.push(shuffleDep);
                        waitingForVisit.push(shuffleDep.rdd());
                    }
                });
            }
        }
        return ancestors;
    }

    private ShuffleMapStage createShuffleMapStage(ShuffleDependency<?, ?, ?> shuffleDep, int jobId) {
        RDD<?> rdd = shuffleDep.rdd();
        int numTasks = rdd.partitions().size();
        List<Stage> parents = getOrCreateParentStages(rdd, jobId);
        int id = nextStageId.getAndIncrement();
        ShuffleMapStage stage = new ShuffleMapStage(
                id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep, mapOutputTracker);

        stageIdToStage.put(id, stage);
        shuffleIdToMapStage.put(shuffleDep.shuffleId, stage);
        updateJobIdStageIdMaps(jobId, Lists.newArrayList(stage));

        if (!mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
            // Kind of ugly: need to register RDDs with the cache and map output tracker here
            // since we can't do it in the RDD constructor because # of partitions is unknown
            LOGGER.info("注册RDD[rddId = {}, callSite = {}]", rdd.id, rdd.creationSite.shortForm);
            mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions().size());
        }
        return stage;
    }

    private void updateJobIdStageIdMaps(int jobId, List<Stage> stages) {
        if (CollectionUtils.isNotEmpty(stages)) {
            Stage s = stages.get(0);
            s.jobIds.add(jobId);
            Set<Integer> stageIds = jobIdToStageIds.get(jobId);
            if (stageIds == null) {
                stageIds = Sets.newHashSet();
                jobIdToStageIds.put(jobId, stageIds);
            }
            stageIds.add(s.id);
            List<Stage> parentsWithoutThisJobId = s.parents.stream()
                                                           .filter(ps -> !ps.jobIds.contains(jobId))
                                                           .collect(Collectors.toList());
            updateJobIdStageIdMaps(jobId, parentsWithoutThisJobId);
        }
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
                JobSubmitted jobSubmitted = (JobSubmitted) event;
                dagScheduler.handleJobSubmitted(jobSubmitted.jobId,
                                                jobSubmitted.finalRDD,
                                                jobSubmitted.func,
                                                jobSubmitted.partitions,
                                                jobSubmitted.callSite,
                                                jobSubmitted.listener,
                                                jobSubmitted.properties);
            }
        }
    }
}
