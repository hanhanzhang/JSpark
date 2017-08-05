package com.sdu.spark.scheduler;

import com.sdu.spark.utils.CallSite;

import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class ActiveJob {
    public int jobId;
    public Stage finalStage;
    public CallSite callSite;
    public JobListener listener;
    public Properties properties;

    public int numPartitions = 0;
    public boolean[] finished;

    public ActiveJob(int jobId, Stage finalStage, CallSite callSite,
                     JobListener listener, Properties properties) {
        this.jobId = jobId;
        this.finalStage = finalStage;
        this.callSite = callSite;
        this.listener = listener;
        this.properties = properties;

        if (finalStage instanceof ResultStage) {
            this.numPartitions = ((ResultStage) finalStage).partitions.size();
        } else if (finalStage instanceof ShuffleMapStage) {
           this.numPartitions = ((ShuffleMapStage) finalStage).rdd.partitions().size();
        }

        this.finished = new boolean[this.numPartitions];
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ActiveJob) {
            return ((ActiveJob) obj).jobId == this.jobId;
        }
        return false;
    }
}
