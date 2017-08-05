package com.sdu.spark.scheduler;

import com.google.common.collect.Lists;
import com.sdu.spark.TaskContext;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.rdd.Transaction;
import com.sdu.spark.utils.CallSite;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class ResultStage extends Stage  {

    public List<Integer> partitions;

    @Setter
    @Getter
    private ActiveJob activeJob;

    public ResultStage(int id, RDD<?> rdd, Transaction<Pair<TaskContext, Iterator<?>>, ?> func,
                       List<Integer> partitions, List<Stage> parents, int firstJobId, CallSite callSite) {
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
}
