package com.sdu.spark.scheduler;

import com.sdu.spark.utils.Clock;

/**
 * @author hanhan.zhang
 * */
public class TaskSetManager implements Schedulable {

    public TaskSchedulerImpl sched;
    public TaskSet taskSet;
    public int maxTaskFailures;
    public BlacklistTracker blacklistTracker;
    public Clock clock;

    public boolean isZombie = false;

    public TaskSetManager(TaskSchedulerImpl sched, TaskSet taskSet, int maxTaskFailures,
                          BlacklistTracker blacklistTracker) {
        this(sched, taskSet, maxTaskFailures, blacklistTracker, new Clock.SystemClock());
    }

    public TaskSetManager(TaskSchedulerImpl sched, TaskSet taskSet, int maxTaskFailures,
                          BlacklistTracker blacklistTracker, Clock clock) {
        this.sched = sched;
        this.taskSet = taskSet;
        this.maxTaskFailures = maxTaskFailures;
        this.blacklistTracker = blacklistTracker;
        this.clock = clock;
    }

    @Override
    public void addSchedulable(Schedulable schedulable) {

    }

    public void abort(String msg) {
        throw new UnsupportedOperationException("");
    }

    /** If the given task ID is in the set of running tasks, removes it. */
    public void removeRunningTask(long tid) {
        throw new UnsupportedOperationException("");
    }
}
