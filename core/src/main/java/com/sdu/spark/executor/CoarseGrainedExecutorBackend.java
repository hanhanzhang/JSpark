package com.sdu.spark.executor;

import com.sdu.spark.SparkEnv;
import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.scheduler.TaskState;
import com.sdu.spark.serializer.SerializerInstance;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class CoarseGrainedExecutorBackend extends RpcEndPoint implements ExecutorBackend {

    public RpcEnv rpcEnv;
    public String driverAddress;
    public String executorId;
    public String hostname;
    public int cores;
    public List<URL> userClassPath;
    public SparkEnv env;

    private Executor executor;
    private RpcEndPointRef driver;
    private SerializerInstance ser;

    public CoarseGrainedExecutorBackend(RpcEnv rpcEnv, String driverAddress, String executorId, String hostname,
                                        int cores, List<URL> userClassPath, SparkEnv env) {
        this.rpcEnv = rpcEnv;
        this.driverAddress = driverAddress;
        this.executorId = executorId;
        this.hostname = hostname;
        this.cores = cores;
        this.userClassPath = userClassPath;
        this.env = env;
        this.ser = this.env.serializer.newInstance();
    }

    @Override
    public RpcEndPointRef self() {
        return rpcEnv.endPointRef(this);
    }

    @Override
    public void onStart() {

    }

    @Override
    public void receive(Object msg) {

    }

    @Override
    public void statusUpdate(long taskId, TaskState state, ByteBuffer data) {

    }
}
