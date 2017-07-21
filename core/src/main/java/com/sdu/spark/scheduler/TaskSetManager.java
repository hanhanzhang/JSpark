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

    public TaskSetManager(TaskSchedulerImpl sched, TaskSet taskSet, int maxTaskFailures) {
        this(sched, taskSet, maxTaskFailures, null, new Clock.SystemClock());
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
}
