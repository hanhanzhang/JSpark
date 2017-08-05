package com.sdu.spark.scheduler;

import com.sdu.spark.TaskContext;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.rdd.Transaction;
import com.sdu.spark.utils.CallSite;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public interface DAGSchedulerEvent {

    @AllArgsConstructor
    class JobSubmitted<T, U> implements DAGSchedulerEvent {
        public int jobId;
        public RDD<T> finalRDD;
        public Transaction<Pair<TaskContext, Iterator<T>>, U> func;
        public List<Integer> partitions;
        public CallSite callSite;
        public JobListener listener;
        public Properties properties;
    }

}
