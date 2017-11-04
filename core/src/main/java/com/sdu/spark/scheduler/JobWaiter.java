package com.sdu.spark.scheduler;

import com.google.common.util.concurrent.SettableFuture;
import com.sdu.spark.scheduler.action.ResultHandler;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link JobWaiter}标记作业运行状态
 *
 * @author hanhan.zhang
 * */
public class JobWaiter<U> implements JobListener {

    private DAGScheduler dagScheduler;
    public int jobId;
    // 作业数 = 分区数
    private int totalTasks;
    private final ResultHandler<U> resultHandler;

    private AtomicInteger finishedTasks = new AtomicInteger(0);

    private SettableFuture<Boolean> jobPromise = SettableFuture.create();

    public JobWaiter(DAGScheduler dagScheduler,
                     int jobId,
                     int totalTasks,
                     ResultHandler<U> resultHandler) {
        this.dagScheduler = dagScheduler;
        this.jobId = jobId;
        this.totalTasks = totalTasks;
        this.resultHandler = resultHandler;
    }

    @SuppressWarnings(value = "unchecked")
    @Override
    public void taskSucceeded(int index, Object result) {
        synchronized (resultHandler) {
            resultHandler.callback(index, (U) result);
        }
        if (finishedTasks.incrementAndGet() == totalTasks) {
            jobPromise.set(true);
        }
    }

    @Override
    public void jobFailed(Exception exception) {
        jobPromise.setException(exception);
    }

    public boolean jobFinished() {
        return jobPromise.isDone();
    }

    public void cancel() {
        dagScheduler.cancelJob(jobId, "");
    }

    public Future<Boolean> completionFuture() {
        return jobPromise;
    }
}
