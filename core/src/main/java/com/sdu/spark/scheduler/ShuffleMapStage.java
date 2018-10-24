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

import static java.lang.String.format;

/**
 * @author hanhan.zhang
 * */
public class ShuffleMapStage extends Stage {

    private MapOutputTrackerMaster mapOutputTrackerMaster;
    private ShuffleDependency<?, ?, ?> shuffleDep;

    private List<ActiveJob> mapStageJobs;
    private Set<Integer> pendingPartitions;

    public ShuffleMapStage(int id,
                           RDD<?> rdd,
                           int numTasks,
                           List<Stage> parents,
                           int firstJobId,
                           CallSite callSite,
                           ShuffleDependency<?, ?, ?> shuffleDep,
                           MapOutputTrackerMaster mapOutputTrackerMaster) {
        super(id, rdd, numTasks, parents, firstJobId, callSite);
        this.mapOutputTrackerMaster = mapOutputTrackerMaster;
        this.shuffleDep = shuffleDep;
        this.pendingPartitions = Sets.newHashSet();
        this.mapStageJobs = Lists.newLinkedList();
    }

    public void addActiveJob(ActiveJob activeJob) {
        mapStageJobs.add(activeJob);
    }

    public void removeActiveJob(ActiveJob activeJob) {
        mapStageJobs.remove(activeJob);
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

    public int numAvailableOutputs() {
        return mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId());
    }

    /** ShuffleMapStage的所有分区的Map任务都成功执行后, ShuffleMapStage才可用 */
    public boolean isAvailable() {
        return numAvailableOutputs() == getNumPartitions();
    }

    public List<ActiveJob> mapStageJobs() {
        return mapStageJobs;
    }

    @Override
    public List<Integer> findMissingPartitions() {
        return Arrays.asList(mapOutputTrackerMaster.findMissingPartitions(shuffleDep.shuffleId()));
    }

    @Override
    public String toString() {
        return format("ShuffleMapStage %d", getId());
    }
}
