package com.sdu.spark.scheduler;

import com.sdu.spark.executor.ExecutorExitCode.*;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.scheduler.action.RDDAction;
import com.sdu.spark.utils.CallSite;

import java.util.List;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public interface DAGSchedulerEvent {

    class JobSubmitted<T, U> implements DAGSchedulerEvent {
        public int jobId;
        public RDD<T> finalRDD;
        public RDDAction<T, U> rddAction;
        public List<Integer> partitions;
        public CallSite callSite;
        public JobListener listener;
        public Properties properties;

        public JobSubmitted(int jobId,
                            RDD<T> finalRDD,
                            RDDAction<T, U> rddAction,
                            List<Integer> partitions,
                            CallSite callSite,
                            JobListener listener,
                            Properties properties) {
            this.jobId = jobId;
            this.finalRDD = finalRDD;
            this.rddAction = rddAction;
            this.partitions = partitions;
            this.callSite = callSite;
            this.listener = listener;
            this.properties = properties;
        }
    }

    class JobCancelled implements DAGSchedulerEvent {
        public int jobId;
        public String reason;

        public JobCancelled(int jobId, String reason) {
            this.jobId = jobId;
            this.reason = reason;
        }
    }

    class ExecutorLost implements DAGSchedulerEvent {
        public String execId;
        public ExecutorLossReason reason;

        public ExecutorLost(String execId, ExecutorLossReason reason) {
            this.execId = execId;
            this.reason = reason;
        }
    }
}
