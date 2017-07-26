package com.sdu.spark.scheduler.cluster;

import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEndPointRef;

/**
 * @author hanhan.zhang
 * */
public class ExecutorData extends ExecutorInfo {

    public RpcEndPointRef executorEndpoint;
    public RpcAddress executorAddress;
    public int freeCores;

    public ExecutorData(String executorHost, int totalCores, RpcEndPointRef executorEndpoint,
                        RpcAddress executorAddress, int freeCores) {
        super(executorHost, totalCores);
        this.executorEndpoint = executorEndpoint;
        this.executorAddress = executorAddress;
        this.freeCores = freeCores;
    }

}
