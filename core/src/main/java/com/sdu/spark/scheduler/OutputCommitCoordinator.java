package com.sdu.spark.scheduler;

import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OutputCommitCoordinator}
 *
 * @author hanhan.zhang 
 * */
public class OutputCommitCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutputCommitCoordinator.class);

    private SparkConf conf;
    private boolean isDriver;

    public RpcEndPointRef coordinatorRef;

    public OutputCommitCoordinator(SparkConf conf, boolean isDriver) {
        this.conf = conf;
        this.isDriver = isDriver;
    }

    public boolean handleAskPermissionToCommit(int stageId, int partition, int attemptNumber) {
        throw new UnsupportedOperationException("");
    }

    public void stop() {
        throw new UnsupportedOperationException("");
    }
}
