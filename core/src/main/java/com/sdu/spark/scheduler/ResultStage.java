package com.sdu.spark.scheduler;

import com.google.common.collect.Lists;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.scheduler.action.PartitionFunction;
import com.sdu.spark.utils.CallSite;

import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

/**
 * @author hanhan.zhang
 * */
public class ResultStage extends Stage  {

    private List<Integer> partitions;

    private ActiveJob activeJob;

    /** RDD分区计算函数 */
    private PartitionFunction<?, ?> func;

    public ResultStage(int id,
                       RDD<?> rdd,
                       PartitionFunction<?, ?> partitionFunction,
                       List<Integer> partitions,
                       List<Stage> parents,
                       int firstJobId,
                       CallSite callSite) {
        super(id, rdd, partitions.size(), parents, firstJobId, callSite);
        this.func = partitionFunction;
        this.partitions = partitions;
    }

    /** 返回当前Job的所有分区中还未完成的分区索引. */
    @Override
    public List<Integer> findMissingPartitions() {
        if (activeJob != null) {
            List<Integer> unfinished = Lists.newLinkedList();
            for (int i = 0; i < activeJob.getNumPartitions(); ++i) {
                if (!activeJob.isPartitionTaskFinished(i)) {
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

    public PartitionFunction<?, ?> getJobAction() {
        return func;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    public ActiveJob getActiveJob() {
        return activeJob;
    }

    public void setActiveJob(ActiveJob activeJob) {
        this.activeJob = activeJob;
    }

    @Override
    public String toString() {
        return format("ResultStage %d", getId());
    }
}
