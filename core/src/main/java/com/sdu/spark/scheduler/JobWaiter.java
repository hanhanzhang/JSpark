package com.sdu.spark.scheduler;

import com.google.common.util.concurrent.SettableFuture;
import com.sdu.spark.scheduler.action.PartitionResultHandler;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JobWaiter用于等待整个作业执行完毕, 然后调用给定的处理函数对返回结果进行处理.
 *
 * @author hanhan.zhang
 * */
public class JobWaiter<U> implements JobListener {

    private DAGScheduler dagScheduler;
    private int jobId;
    /** 等待完成的作业数(作业数=分区数) */
    private int totalTasks;
    /** 分区结果处理器 */
    private final PartitionResultHandler<U> resultHandler;
    /** 已完成的作业数 */
    private AtomicInteger finishedTasks = new AtomicInteger(0);

    private SettableFuture<Boolean> jobPromise = SettableFuture.create();

    public JobWaiter(DAGScheduler dagScheduler,
                     int jobId,
                     int totalTasks,
                     PartitionResultHandler<U> resultHandler) {
        this.dagScheduler = dagScheduler;
        this.jobId = jobId;
        this.totalTasks = totalTasks;
        this.resultHandler = resultHandler;
    }

    @SuppressWarnings(value = "unchecked")
    @Override
    public void taskSucceeded(int index, Object result) {
        synchronized (resultHandler) {
            resultHandler.handle(index, (U) result);
        }
        if (finishedTasks.incrementAndGet() == totalTasks) {
            jobPromise.set(true);
        }
    }

    @Override
    public void jobFailed(Exception exception) {
        jobPromise.setException(exception);
    }

    public int getJobId() {
        return jobId;
    }

    public boolean jobFinished() {
        return jobPromise.isDone();
    }

    public void cancel() {
        dagScheduler.cancelJob(jobId, null);
    }

    public Future<Boolean> completionFuture() {
        return jobPromise;
    }
}
