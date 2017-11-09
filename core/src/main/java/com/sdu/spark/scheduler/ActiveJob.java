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
    public boolean[] finished;

    /**
     * @param jobId A unique ID for this job.
     * @param finalStage The stage that this job computes (either a ResultStage for an action or a
     *                   ShuffleMapStage for submitMapStage).
     * @param callSite Where this job was initiated in the user's program (shown on UI).
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
            this.numPartitions = ((ResultStage) finalStage).partitions.size();
        } else if (finalStage instanceof ShuffleMapStage) {
           this.numPartitions = ((ShuffleMapStage) finalStage).rdd.partitions().length;
        }

        this.finished = new boolean[this.numPartitions];
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

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ActiveJob) {
            return ((ActiveJob) obj).jobId == this.jobId;
        }
        return false;
    }
}
