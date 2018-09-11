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
@SuppressWarnings("unused")
public abstract class Stage {

    public int id;
    public RDD<?> rdd;
    public int numTasks;
    public List<Stage> parents;
    public int firstJobId;

    private Set<Integer> jobIds = Sets.newHashSet();
    private int nextAttemptId = 0;
    public int numPartitions;
    public String name;
    public String details;
    private StageInfo latestInfo;
    private Set<Integer> fetchFailedAttemptIds = Sets.newHashSet();



    public Stage(int id,
                 RDD<?> rdd,
                 int numTasks,
                 List<Stage> parents,
                 int firstJobId) {
        this.id = id;
        this.rdd = rdd;
        this.numTasks = numTasks;
        this.parents = parents;
        this.firstJobId = firstJobId;

        this.numPartitions = this.rdd.partitions().length;
        this.latestInfo = StageInfo.fromStage(this, nextAttemptId);
    }

    public void clearFailures() {
        fetchFailedAttemptIds.clear();
    }

    public void makeNewStageAttempt(int numPartitionsToCompute) {
        makeNewStageAttempt(numPartitionsToCompute, new TaskLocation[0][0]);
    }

    public void makeNewStageAttempt(int numPartitionsToCompute,
                                    TaskLocation[][] taskLocalityPreferences) {
        // TODO: TaskMetric
        latestInfo = StageInfo.fromStage(this,
                                         nextAttemptId,
                                         numPartitionsToCompute,
                                         taskLocalityPreferences);
        nextAttemptId += 1;
    }

    public StageInfo latestInfo() {
        return latestInfo;
    }

    public Set<Integer> jobIds() {
        return jobIds;
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
