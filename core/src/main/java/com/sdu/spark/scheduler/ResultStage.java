package com.sdu.spark.scheduler;

import com.google.common.collect.Lists;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.scheduler.action.RDDAction;
import com.sdu.spark.utils.CallSite;

import java.util.Collections;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class ResultStage extends Stage  {

    public List<Integer> partitions;

    private ActiveJob activeJob;

    public ResultStage(int id,
                       RDD<?> rdd,
                       RDDAction<?, ?> rddAction,
                       List<Integer> partitions,
                       List<Stage> parents,
                       int firstJobId,
                       CallSite callSite) {
        super(id, rdd, partitions.size(), parents, firstJobId, callSite);
        this.partitions = partitions;
    }

    @Override
    public List<Integer> findMissingPartitions() {
        if (activeJob != null) {
            List<Integer> unfinished = Lists.newLinkedList();
            for (int i = 0; i < activeJob.numPartitions; ++i) {
                if (!activeJob.finished[i]) {
                    unfinished.add(i);
                }
            }
            return unfinished;
        }
        return Collections.emptyList();
    }

    public void removeActiveJob() {
        activeJob = null;
    }

    public ActiveJob getActiveJob() {
        return activeJob;
    }

    public void setActiveJob(ActiveJob activeJob) {
        this.activeJob = activeJob;
    }

    @Override
    public String toString() {
        return String.format("ResultStage %d", id);
    }
}
