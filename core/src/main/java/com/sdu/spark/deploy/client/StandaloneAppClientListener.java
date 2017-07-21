package com.sdu.spark.deploy.client;

/**
 * @author hanhan.zhang
 * */
public interface StandaloneAppClientListener {

    void connected(String appId);

    /** Disconnection may be a temporary state, as we fail over to a new Master. */
    void disconnected();

    /** An application death is an unrecoverable failure condition. */
    void dead(String reason);

    void executorAdded(String fullId, String workerId, String hostPort, int cores, int memory);

    void executorRemove(String fullId, String message, int exitStatus, boolean workerLost);

    void workerRemoved(String workerId, String host, String message);

}
