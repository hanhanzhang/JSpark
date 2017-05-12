package com.sdu.spark.rpc.deploy;

import com.sdu.spark.rpc.*;
import com.sdu.spark.rpc.deploy.DeployMessage.*;
import com.sdu.spark.rpc.deploy.MasterMessage.*;
import com.sdu.spark.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 集群Master节点, 负责管理集群及Application信息
 *
 * @author hanhan.zhang
 * */
public class Master extends RpcEndPoint {

    public static final Logger LOGGER = LoggerFactory.getLogger(Master.class);

    private static final String END_POINT_NAME = "RpcMaster";

    // RpcEnv
    private RpcEnv rpcEnv;
    /**
     * RpcMaster配置
     * */
    private RpcConfig config;
    /**
     * Master节点地址
     * */
    private RpcAddress address;
    /**
     * 集群工作节点
     * */
    private Set<WorkerInfo> workers = new HashSet<>();
    /**
     * 工作节点地址信息[key = 工作节点地址, value = 工作节点信息]
     * */
    private Map<RpcAddress, WorkerInfo> addressToWorker = new HashMap<>();
    /**
     * 工作节点标识[key = 工作节点唯一标识, value = 工作节点信息]
     * */
    private Map<String, WorkerInfo> idToWorker = new HashMap<>();

    private ScheduledExecutorService messageThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread");

    private ScheduledFuture<?> checkWorkerTimeoutTask;

    public Master(RpcConfig config, RpcEnv rpcEnv, RpcAddress address) {
        this.config = config;
        this.rpcEnv = rpcEnv;
        this.address = address;
    }

    @Override
    public RpcEndPointRef self() {
        return rpcEnv.setRpcEndPointRef(END_POINT_NAME, this);
    }

    @Override
    public void onStart() {
        LOGGER.info("start rpc master at {}", address.toRpcURL());
        messageThread.scheduleWithFixedDelay(() -> self().send(new CheckForWorkerTimeOut()),
                                             config.getCheckWorkerTimeout(),
                                             config.getCheckWorkerTimeout(),
                                             TimeUnit.SECONDS);
    }

    @Override
    public void onEnd() {

    }

    @Override
    public void onStop() {
        if (checkWorkerTimeoutTask != null) {
            checkWorkerTimeoutTask.cancel(true);
        }
    }

    @Override
    public void onConnect(RpcAddress rpcAddress) {

    }

    @Override
    public void onDisconnect(RpcAddress rpcAddress) {

    }

    @Override
    public void receive(Object msg) {
        if (msg instanceof CheckForWorkerTimeOut) {

        } else if (msg instanceof WorkerHeartbeat) {
            WorkerHeartbeat heartbeat = (WorkerHeartbeat) msg;
            String workerId = heartbeat.getWorkerId();
            WorkerInfo workerInfo = idToWorker.get(workerId);
            if (workerInfo == null) {

            } else {
                workerInfo.setLastHeartbeat(System.currentTimeMillis());
            }
        } else if (msg instanceof RegisterWorker) {
            RegisterWorker registerWorker = (RegisterWorker) msg;
            if (idToWorker.containsKey(registerWorker.getWorkerId())) {
                registerWorker.getWorker().send(new RegisterWorkerFailed("Duplicate worker ID"));
            } else {
                WorkerInfo workerInfo = new WorkerInfo(registerWorker.getWorkerId(), registerWorker.getHost(),
                                                       registerWorker.getPort(), registerWorker.getCores(),
                                                       registerWorker.getMemory(), registerWorker.getWorker());
                registerWorker(workerInfo);
            }
        }
    }

    private boolean registerWorker(WorkerInfo worker) {
        // 删除已挂掉的worker及当前注册work地址相同
        workers.stream().filter(w -> (w.getHost() == worker.getHost() && w.getPort() == worker.getPort()) &&
                                     (w.getState() == WorkerState.DEAD))
                        .forEach(w -> workers.remove(w));
        RpcAddress address = worker.getEndPointRef().address();
        if (addressToWorker.containsKey(address)) {
            WorkerInfo oldWorker = addressToWorker.get(address);
            if (oldWorker.getState() == WorkerState.UNKNOWN) {
                removeWorker(oldWorker);
            } else {
                LOGGER.info("Attempted to re-register worker at same address: {}", address);
                return true;
            }
        }
        idToWorker.put(worker.getWorkerId(), worker);
        addressToWorker.put(address, worker);
        workers.add(worker);
        return true;
    }

    private void removeWorker(WorkerInfo worker) {

    }
}
