package com.sdu.spark.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sdu.spark.MapOutputTrackerMaster;
import com.sdu.spark.ShuffleDependency;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.utils.CallSite;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @author hanhan.zhang
 * */
public class ShuffleMapStage extends Stage {

    private MapOutputTrackerMaster mapOutputTrackerMaster;
    private ShuffleDependency<?, ?, ?> shuffleDep;

    public List<ActiveJob> mapStageJobs;
    private Set<Integer> pendingPartitions;

    public ShuffleMapStage(int id,
                           RDD<?> rdd,
                           int numTasks,
                           List<Stage> parents,
                           int firstJobId,
                           ShuffleDependency<?, ?, ?> shuffleDep,
                           MapOutputTrackerMaster mapOutputTrackerMaster) {
        super(id, rdd, numTasks, parents, firstJobId);
        this.mapOutputTrackerMaster = mapOutputTrackerMaster;
        this.shuffleDep = shuffleDep;
        this.pendingPartitions = Sets.newHashSet();
        this.mapStageJobs = Lists.newLinkedList();
    }

    @Override
    public List<Integer> findMissingPartitions() {
        return Arrays.asList(mapOutputTrackerMaster.findMissingPartitions(shuffleDep.shuffleId()));
    }

    @Override
    public String toString() {
        return String.format("ShuffleMapStage[stageId = %s]", id);
    }

    public void addActiveJob(ActiveJob activeJob) {
        mapStageJobs.add(activeJob);
    }

    public ShuffleDependency<?, ?, ?> getShuffleDep() {
        return shuffleDep;
    }

    public void clearWaitPartitionTask() {
        pendingPartitions.clear();
    }

    public boolean isWaitPartitionTaskFinished() {
        return pendingPartitions.isEmpty();
    }

    public void addWaitPartitionTask(int partitionId) {
        pendingPartitions.add(partitionId);
    }

    public void markPartitionTaskFinished(int partitionId) {
        pendingPartitions.remove(partitionId);
    }

    public void removeActiveJob(ActiveJob activeJob) {
        mapStageJobs.remove(activeJob);
    }

    public int numAvailableOutputs() {
        return mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId());
    }

    public boolean isAvailable() {
        return numAvailableOutputs() == numPartitions;
    }
}
