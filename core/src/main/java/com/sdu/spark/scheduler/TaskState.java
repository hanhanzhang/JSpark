package com.sdu.spark.scheduler;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * @author hanhan.zhang
 * */
public enum TaskState {

    LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST;

    private static Set<TaskState> FINISHED_STATES = Sets.newHashSet(FINISHED, FAILED, KILLED, LOST);

    public boolean isFailed(TaskState state) {
        return state == LOST || state == FAILED;
    }

    public boolean isFinished(TaskState state) {
        return FINISHED_STATES.contains(state);
    }

}
