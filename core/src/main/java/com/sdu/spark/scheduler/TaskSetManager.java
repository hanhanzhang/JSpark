package com.sdu.spark.scheduler;

import com.google.common.collect.Maps;
import com.sdu.spark.scheduler.TaskEndReason.*;
import com.sdu.spark.utils.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.sdu.spark.utils.Utils.bytesToString;
import static com.sdu.spark.utils.Utils.getMaxResultSize;
import static java.lang.String.format;

/**
 * @author hanhan.zhang
 * */
public class TaskSetManager implements Schedulable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskSetManager.class);

    public TaskSchedulerImpl sched;
    public TaskSet taskSet;
    private boolean[] successful;
    public int maxTaskFailures;
    public BlacklistTracker blacklistTracker;
    public Clock clock;

    public boolean isZombie = false;

    // Task index, start and finish time for each task attempt (indexed by task ID)
    private Map<Long, TaskInfo> taskInfos;

    private long totalResultSize = 0L;
    private int calculatedTasks = 0;
    private long maxResultSize;

    public TaskSetManager(TaskSchedulerImpl sched,
                          TaskSet taskSet,
                          int maxTaskFailures,
                          BlacklistTracker blacklistTracker) {
        this(sched, taskSet, maxTaskFailures, blacklistTracker, new Clock.SystemClock());
    }

    public TaskSetManager(TaskSchedulerImpl sched,
                          TaskSet taskSet,
                          int maxTaskFailures,
                          BlacklistTracker blacklistTracker,
                          Clock clock) {
        this.sched = sched;
        this.taskSet = taskSet;
        this.successful = new boolean[taskSet.tasks.length];
        this.maxTaskFailures = maxTaskFailures;
        this.blacklistTracker = blacklistTracker;
        this.clock = clock;

        this.maxResultSize = getMaxResultSize(this.sched.sc.conf);
        this.taskInfos = Maps.newHashMap();
    }

    @Override
    public void addSchedulable(Schedulable schedulable) {

    }

    /**
     * Check whether has enough quota to fetch the result with `size` bytes
     * */
    public boolean canFetchMoreResults(long size) {
        totalResultSize += size;
        calculatedTasks += 1;
        if (maxResultSize > 0 && totalResultSize > maxResultSize) {
            String msg = format("Total size of serialized results of %d tasks (%s) is bigger than " +
                    "spark.driver.maxResultSize(%s)", calculatedTasks, bytesToString(totalResultSize),
                    bytesToString(maxResultSize));
            LOGGER.error(msg);
            abort(msg);
            return false;
        }
        return true;
    }

    public void handleTaskGettingResult(long tid) {
        TaskInfo info = taskInfos.get(tid);
        info.markGettingResult(clock.getTimeMillis());
        sched.dagScheduler.taskGettingResult(info);
    }

    public void handleFailedTask(long tid,
                                 TaskState state,
                                 TaskFailedReason reason) {

    }

    public void handleSuccessfulTask(long tid,
                                     DirectTaskResult<?> taskResult) {

    }

    public boolean someAttemptSucceeded(long tid) {
        return successful[taskInfos.get(tid).index];
    }

    public void abort(String msg) {
        // TODO: Kill running tasks if we were not terminated due to a Mesos error
        sched.dagScheduler.taskSetFailed(taskSet, msg, null);
        isZombie = true;
        maybeFinishTaskSet();
    }

    private void maybeFinishTaskSet() {
//        if (isZombie && runningTasks == 0) {
//            sched.taskSetFinished(this)
//            if (tasksSuccessful == numTasks) {
//                blacklistTracker.foreach(_.updateBlacklistForSuccessfulTaskSet(
//                        taskSet.stageId,
//                        taskSet.stageAttemptId,
//                        taskSetBlacklistHelperOpt.get.execToFailures))
//            }
//        }
    }

    /** If the given task ID is in the set of running tasks, removes it. */
    public void removeRunningTask(long tid) {
        throw new UnsupportedOperationException("");
    }
}
