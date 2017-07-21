package com.sdu.spark.scheduler;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public class WorkerOffer implements Serializable {

    public String executorId;
    public String host;
    public int cores;

    public WorkerOffer(String executorId, String host, int cores) {
        this.executorId = executorId;
        this.host = host;
        this.cores = cores;
    }
}
