package com.sdu.spark.scheduler;

import com.sdu.spark.rdd.Transaction;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.Future;

/**
 *
 * @author hanhan.zhang
 * */
public class JobWaiter<T> implements JobListener {

    private DAGScheduler dagScheduler;
    public int jobId;
    private int totalTasks;
    private Transaction<Pair<Integer, T>, Void> resultHandler;

    public JobWaiter(DAGScheduler dagScheduler, int jobId, int totalTasks,
                     Transaction<Pair<Integer, T>, Void> resultHandler) {
        this.dagScheduler = dagScheduler;
        this.jobId = jobId;
        this.totalTasks = totalTasks;
        this.resultHandler = resultHandler;
    }

    @Override
    public void taskSucceeded(Integer index, Object result) {

    }

    @Override
    public void jobFailed(Exception exception) {

    }

    public Future<Boolean> completionFuture() {
        throw new UnsupportedOperationException("");
    }
}
