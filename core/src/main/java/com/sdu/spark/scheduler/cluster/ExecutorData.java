package com.sdu.spark.scheduler.cluster;

import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEndpointRef;

/**
 * @author hanhan.zhang
 * */
public class ExecutorData extends ExecutorInfo {

    public RpcEndpointRef executorEndpoint;
    public RpcAddress executorAddress;
    public int freeCores;

    public ExecutorData(String executorHost, int totalCores, RpcEndpointRef executorEndpoint,
                        RpcAddress executorAddress, int freeCores) {
        super(executorHost, totalCores);
        this.executorEndpoint = executorEndpoint;
        this.executorAddress = executorAddress;
        this.freeCores = freeCores;
    }

}
