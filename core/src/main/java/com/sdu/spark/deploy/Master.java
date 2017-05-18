package com.sdu.spark.deploy;

import com.sdu.spark.rpc.*;
import com.sdu.spark.deploy.DeployMessage.*;
import com.sdu.spark.deploy.MasterMessage.*;
import com.sdu.spark.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    public static final String MASTER = "JSparkMaster";

    // RpcEnv
    private RpcEnv rpcEnv;
    /**
     * RpcMaster配置
     * */
    private JSparkConfig config;
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

    public Master(JSparkConfig config, RpcEnv rpcEnv, RpcAddress address) {
        this.config = config;
        this.rpcEnv = rpcEnv;
        this.address = address;
    }

    @Override
    public RpcEndPointRef self() {
        return rpcEnv.setRpcEndPointRef(MASTER, this);
    }

    @Override
    public void onStart() {
        LOGGER.info("start rpc master at {}", address.toSparkURL());
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
        if (msg instanceof ElectedLeader) {                 // Master选主处理

        } else if (msg instanceof CompleteRecovery) {       // Master恢复

        } else if (msg instanceof RevokedLeadership) {

        } else if (msg instanceof CheckForWorkerTimeOut) {
            timeoutDeadWorkers();
        } else if (msg instanceof WorkerHeartbeat) {       // 工作节点心跳消息
            WorkerHeartbeat heartbeat = (WorkerHeartbeat) msg;
            String workerId = heartbeat.workerId;
            WorkerInfo workerInfo = idToWorker.get(workerId);
            if (workerInfo == null) {
                LOGGER.info("Got {} from unregistered worker {}, asking it to re-register.",
                            heartbeat, workerId);
                heartbeat.worker.send(new ReconnectWorker(self()));
            } else {
                workerInfo.setLastHeartbeat(System.currentTimeMillis());
            }
        } else if (msg instanceof RegisterWorker) {       // 注册工作节点
            RegisterWorker registerWorker = (RegisterWorker) msg;
            if (idToWorker.containsKey(registerWorker.workerId)) {
                registerWorker.worker.send(new RegisterWorkerFailed("Duplicate worker ID"));
            } else {
                WorkerInfo workerInfo = new WorkerInfo(registerWorker.workerId, registerWorker.host,
                                                       registerWorker.port, registerWorker.cores,
                                                       registerWorker.memory, registerWorker.worker);
                if (registerWorker(workerInfo)) {
                    registerWorker.worker.send(new RegisteredWorker(self()));
                    // 调度分配应用
                    schedule();
                } else {
                    RpcAddress workerAddress = registerWorker.worker.address();
                    LOGGER.info("Worker registration failed. Attempted to re-register worker at same " +
                            "address: {}", workerAddress);
                    registerWorker.worker.send(new RegisterWorkerFailed("Attempted to re-register worker at same address: "
                            + workerAddress));
                }
            }
        } else if (msg instanceof MasterChangeAcknowledged) {

        } else if (msg instanceof WorkerSchedulerStateResponse) {

        } else if (msg instanceof WorkerLatestState) {

        } else if (msg instanceof RegisterApplication) {    // 注册应用

        } else if (msg instanceof UnregisterApplication) {  // 移除应用

        } else if (msg instanceof ExecutorStateChanged) {   // Executor变化

        } else if (msg instanceof DriverStateChanged) {

        }
    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof RequestSubmitDriver) {           // 请求注册Driver

        } else if (msg instanceof RequestKillDriver) {      // 杀死Driver

        } else if (msg instanceof RequestDriverStatus) {

        } else if (msg instanceof RequestMasterState) {

        } else if (msg instanceof BoundPortsRequest) {

        } else if (msg instanceof RequestExecutors) {

        } else if (msg instanceof KillExecutors) {

        }  else {
            throw new IllegalArgumentException("Unknown message");
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

    private void schedule() {

    }

    /**
     * 摘掉心跳超时工作节点
     * */
    private void timeoutDeadWorkers() {
        Iterator<WorkerInfo> it = workers.iterator();
        while (it.hasNext()) {
            WorkerInfo worker = it.next();
            if (worker.getLastHeartbeat() < System.currentTimeMillis() - config.getWorkerTimeout() &&
                    worker.getState() != WorkerState.DEAD) {
                LOGGER.info("Removing worker {} because we got no heartbeat in {} seconds", worker.getWorkerId(),
                        config.getWorkerTimeout());
                removeWorker(worker);
                it.remove();
            } else {
                if (worker.getLastHeartbeat() < System.currentTimeMillis() - (config.getDeadWorkerPersistenceTimes() + 1) * config.getWorkerTimeout()) {
                    it.remove();
                }
            }
        }
    }
}
