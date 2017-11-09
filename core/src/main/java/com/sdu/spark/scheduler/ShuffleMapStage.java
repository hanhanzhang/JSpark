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
    public ShuffleDependency<?, ?, ?> shuffleDep;

    private List<ActiveJob> mapStageJobs = Lists.newLinkedList();
    public Set<Integer> pendingPartitions = Sets.newHashSet();

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
