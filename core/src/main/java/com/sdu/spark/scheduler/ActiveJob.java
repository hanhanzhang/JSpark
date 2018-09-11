package com.sdu.spark.scheduler;

import com.sdu.spark.utils.CallSite;

import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class ActiveJob {
    private int jobId;
    private Stage finalStage;
    private CallSite callSite;
    private JobListener listener;
    public Properties properties;

    public int numPartitions = 0;
    private int numFinished;
    private boolean[] finished;

    /**
     * @param jobId A unique ID for this job.
     * @param finalStage The stage that this job computes (either a ResultStage for an action or a
     *                   ShuffleMapStage for submitMapStage).
     * @param listener A listener to notify if tasks in this job finish or the job fails.
     * @param properties Scheduling properties attached to the job, such as fair scheduler pool name.
     * */
    public ActiveJob(int jobId,
                     Stage finalStage,
                     JobListener listener,
                     Properties properties) {
        this.jobId = jobId;
        this.finalStage = finalStage;
        this.listener = listener;
        this.properties = properties;

        if (finalStage instanceof ResultStage) {
            this.numPartitions = ((ResultStage) finalStage).partitions.size();
        } else if (finalStage instanceof ShuffleMapStage) {
           this.numPartitions = ((ShuffleMapStage) finalStage).rdd.partitions().length;
        }

        this.numFinished = 0;
        this.finished = new boolean[this.numPartitions];
        for (int i = 0; i < this.numPartitions; ++i) {
            this.finished[i] = false;
        }
    }

    public int jobId() {
        return jobId;
    }

    public JobListener listener() {
        return listener;
    }

    public Stage finalStage() {
        return finalStage;
    }

    public boolean isPartitionTaskFinished(int partitionId) {
        return this.finished[partitionId];
    }

    public void markPartitionTaskFinished(int partitionId) {
        this.finished[partitionId] = true;
        this.numFinished += 1;
    }

    public boolean isJobFinished() {
        return numFinished == numPartitions;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ActiveJob) {
            return ((ActiveJob) obj).jobId == this.jobId;
        }
        return false;
    }
}
