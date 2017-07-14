package com.sdu.spark.deploy.worker;

import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.RpcEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hanhan.zhang
 * */
public class WorkerWatcher extends RpcEndPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerWatcher.class);

    private RpcEnv rpcEnv;
    private String workerUrl;
    private boolean isTesting;

    private RpcAddress expectedAddress;

    // Used to avoid shutting down JVM during tests
    // In the normal case, exitNonZero will call `System.exit(-1)` to shutdown the JVM. In the unit
    // test, the user should call `setTesting(true)` so that `exitNonZero` will set `isShutDown` to
    // true rather than calling `System.exit`. The user can check `isShutDown` to know if
    // `exitNonZero` is called.
    private boolean isShutDown = false;

    public WorkerWatcher(RpcEnv rpcEnv, String workerUrl) {
        this(rpcEnv, workerUrl, false);
    }

    public WorkerWatcher(RpcEnv rpcEnv, String workerUrl, boolean isTesting) {
        this.rpcEnv = rpcEnv;
        this.workerUrl = workerUrl;
        this.isTesting = isTesting;

        this.expectedAddress = RpcAddress.fromURI(this.workerUrl);
    }

    @Override
    public RpcEndPointRef self() {
        return rpcEnv.endPointRef(this);
    }

    @Override
    public void onConnect(RpcAddress rpcAddress) {
        if (isWorker(rpcAddress)) {
            LOGGER.info("Remote(address = {})连接Worker(address = {})成功", rpcAddress, expectedAddress);
        }
    }

    @Override
    public void onDisconnect(RpcAddress rpcAddress) {
        if (isWorker(rpcAddress)) {
            exitNonZero();
        }
    }

    private void exitNonZero() {
        if (isTesting) {
            isShutDown = true;
        } else {
            System.exit(-1);
        }
    }

    private boolean isWorker(RpcAddress address) {
        return this.expectedAddress.equals(address);
    }
}
