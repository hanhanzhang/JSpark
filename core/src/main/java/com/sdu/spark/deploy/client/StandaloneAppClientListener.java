package com.sdu.spark.deploy.client;

/**
 * @author hanhan.zhang
 * */
public interface StandaloneAppClientListener {

    /**Spark App注册Master成功*/
    void connected(String appId);

    /** Disconnection may be a temporary state, as we fail over to a new Master. */
    void disconnected();

    /** An application death is an unrecoverable failure condition. */
    void dead(String reason);

    /***Spark Master启动Executor*/
    void executorAdded(String fullId, String workerId, String hostPort, int cores, int memory);

    /**Spark Executor移除*/
    void executorRemoved(String fullId, String message, int exitStatus, boolean workerLost);

    void workerRemoved(String workerId, String host, String message);

}
