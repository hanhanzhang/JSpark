package com.sdu.spark.scheduler.cluster;

import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEndPointRef;

/**
 * @author hanhan.zhang
 * */
public class ExecutorData extends ExecutorInfo {

    public RpcEndPointRef executorEndpoint;
    public RpcAddress executorAddress;
    public String executorHost;
    public int freeCores;
    public int totalCores;

    public ExecutorData(String executorHost, int totalCores, RpcEndPointRef executorEndpoint,
                        RpcAddress executorAddress, String executorHost1, int freeCores, int totalCores1) {
        super(executorHost, totalCores);
        this.executorEndpoint = executorEndpoint;
        this.executorAddress = executorAddress;
        this.executorHost = executorHost1;
        this.freeCores = freeCores;
        this.totalCores = totalCores1;
    }

}
