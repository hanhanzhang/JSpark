package com.sdu.spark.scheduler;

import com.google.common.collect.Sets;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.utils.CallSite;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 * @author hanhan.zhang
 * */
public abstract class Stage {

    public int id;
    public RDD<?> rdd;
    public int numTasks;
    public List<Stage> parents;
    public int firstJobId;
    public CallSite callSite;

    public Set<Integer> jobIds = Sets.newHashSet();
    private Set<Integer> fetchFailedAttemptIds = Sets.newHashSet();
    private int nextAttemptId = 0;
    public int numPartitions;
    public String name;
    public String details;
    public StageInfo latestInfo;



    public Stage(int id, RDD<?> rdd, int numTasks, List<Stage> parents,
                 int firstJobId, CallSite callSite) {
        this.id = id;
        this.rdd = rdd;
        this.numTasks = numTasks;
        this.parents = parents;
        this.firstJobId = firstJobId;
        this.callSite = callSite;

        this.numPartitions = this.rdd.partitions().size();
        this.name = this.callSite.shortForm;
        this.details = this.callSite.longForm;
        this.latestInfo = StageInfo.fromStage(this, nextAttemptId);
    }

    public void clearFailures() {
        fetchFailedAttemptIds.clear();
    }

    public void makeNewStageAttempt(int numPartitionsToCompute) {
        makeNewStageAttempt(numPartitionsToCompute, Collections.emptyList());
    }

    public void makeNewStageAttempt(int numPartitionsToCompute, List<TaskLocation> taskLocalityPreferences) {
        latestInfo = StageInfo.fromStage(
                this, nextAttemptId, numPartitionsToCompute, taskLocalityPreferences);
        nextAttemptId += 1;
    }

    public abstract List<Integer> findMissingPartitions();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Stage stage = (Stage) o;

        return id == stage.id;

    }

    @Override
    public int hashCode() {
        return id;
    }
}
