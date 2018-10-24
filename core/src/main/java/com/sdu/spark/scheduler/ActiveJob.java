package com.sdu.spark.scheduler;

import com.sdu.spark.utils.CallSite;

import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class ActiveJob {
    /** Job身份标识 */
    private int jobId;
    /** Job最下游Stage */
    private Stage finalStage;
    /** 应用程序调用栈 */
    private CallSite callSite;
    private JobListener listener;
    private Properties properties;

    private int numPartitions = 0;
    private int numFinished;
    /** 标识每个分区的Task是否完成 */
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
                     CallSite callSite,
                     JobListener listener,
                     Properties properties) {
        this.jobId = jobId;
        this.finalStage = finalStage;
        this.callSite = callSite;
        this.listener = listener;
        this.properties = properties;

        if (finalStage instanceof ResultStage) {
            this.numPartitions = ((ResultStage) finalStage).getPartitions().size();
        } else if (finalStage instanceof ShuffleMapStage) {
           this.numPartitions = ((ShuffleMapStage) finalStage).getRdd().partitions().length;
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

    public int getNumPartitions() {
        return numPartitions;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ActiveJob) {
            return ((ActiveJob) obj).jobId == this.jobId;
        }
        return false;
    }
}
