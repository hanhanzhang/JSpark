package com.sdu.spark.storage;

import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.sdu.spark.utils.RpcUtils.getRpcAskTimeout;

/**
 * @author hanhan.zhang
 * */
public class BlockManagerMaster {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockManagerMaster.class);
    public static final String DRIVER_ENDPOINT_NAME = "BlockManagerMaster";

    /**BlockManagerMasterEndpoint的引用*/
    private RpcEndPointRef driverEndpoint;
    private SparkConf conf;
    private boolean isDriver;

    private long timeout;

    public BlockManagerMaster(RpcEndPointRef driverEndpoint, SparkConf conf, boolean isDriver) {
        this.driverEndpoint = driverEndpoint;
        this.conf = conf;
        this.isDriver = isDriver;

        this.timeout = getRpcAskTimeout(this.conf);
    }

    public void removeExecutorAsync(String executorId) {
        throw new UnsupportedOperationException("");
    }

}
