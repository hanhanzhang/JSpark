package com.sdu.spark.deploy;

import com.sdu.spark.rpc.RpcEndpointRef;

import java.io.Serializable;
import java.util.List;

/**
 * JSpark集群部署消息
 *
 * @author hanhan.zhang
 * */
public interface DeployMessage extends Serializable {

    /**
     * 工作节点心跳信息
     * */
    class Heartbeat implements DeployMessage {
        public String workerId;
        public RpcEndpointRef worker;

        public Heartbeat(String workerId, RpcEndpointRef worker) {
            this.workerId = workerId;
            this.worker = worker;
        }
    }

    class SendHeartbeat implements DeployMessage {}

    /**
     * 工作节点注册消息
     * */
    class RegisterWorker implements DeployMessage {
        public String workerId;
        public String host;
        public int port;
        public int cores;
        public long memory;
        public RpcEndpointRef worker;

        public RegisterWorker(String workerId, String host, int port, int cores,
                              long memory, RpcEndpointRef worker) {
            this.workerId = workerId;
            this.host = host;
            this.port = port;
            this.cores = cores;
            this.memory = memory;
            this.worker = worker;
        }
    }

    /**
     * 工作节点注册响应父类
     * */
    interface RegisteredWorkerResponse {}

    /**
     * 工作节点注册响应消息
     * */
    class RegisteredWorker implements DeployMessage, RegisteredWorkerResponse {
        public RpcEndpointRef master;

        public RegisteredWorker(RpcEndpointRef master) {
            this.master = master;
        }
    }

    /**
     * 工作节点注册失败消息
     * */
    class RegisterWorkerFailed implements DeployMessage, RegisteredWorkerResponse {
        public String message;

        public RegisterWorkerFailed(String message) {
            this.message = message;
        }
    }

    /**
     * 工作节点状态消息
     * */
    class WorkerLatestState implements DeployMessage {
        public String workerId;
        public List<ExecutorDescription> executors;
        public List<String> driverIds;

        public WorkerLatestState(String workerId, List<ExecutorDescription> executors,
                                 List<String> driverIds) {
            this.workerId = workerId;
            this.executors = executors;
            this.driverIds = driverIds;
        }
    }

    /**
     * 工作节点资源调度响应消息
     * */
    class WorkerSchedulerStateResponse implements DeployMessage {
        public String workerId;
        public List<ExecutorDescription> executors;
        public List<String> driverIds;

        public WorkerSchedulerStateResponse(String workerId, List<ExecutorDescription> executors,
                                            List<String> driverIds) {
            this.workerId = workerId;
            this.executors = executors;
            this.driverIds = driverIds;
        }
    }


    /**
     * Worker重连消息
     * */
    class ReconnectWorker implements DeployMessage {
        public RpcEndpointRef master;

        public ReconnectWorker(RpcEndpointRef master) {
            this.master = master;
        }
    }

    /**
     * Executor变更消息
     * */
    class ExecutorStateChanged implements DeployMessage {
        public int executorId;
        public String appId;
        public ExecutorState state;
        public String message;
        public int exitStatus;

        public ExecutorStateChanged(int executorId, String appId, ExecutorState state,
                                    String message, int exitStatus) {
            this.executorId = executorId;
            this.appId = appId;
            this.state = state;
            this.message = message;
            this.exitStatus = exitStatus;
        }
    }


    /**
     * Driver变更消息
     * */
    class DriverStateChanged implements DeployMessage {
        public String driverId;
        public DriverState state;
        public Exception exception;

        public DriverStateChanged(String driverId, DriverState state, Exception exception) {
            this.driverId = driverId;
            this.state = state;
            this.exception = exception;
        }
    }
    /**
     * 注册Driver信息
     * */
    class RequestSubmitDriver implements DeployMessage {
        public DriverDescription driverDescription;

        public RequestSubmitDriver(DriverDescription driverDescription) {
            this.driverDescription = driverDescription;
        }
    }

    /**
     * 杀死Driver消息
     * */
    class RequestKillDriver implements DeployMessage {
        public String driverId;

        public RequestKillDriver(String driverId) {
            this.driverId = driverId;
        }
    }

    /**
     * 工作节点启动Driver
     * */
    class LaunchDriver implements DeployMessage {
        public String driverId;
        public DriverDescription desc;

        public LaunchDriver(String driverId, DriverDescription desc) {
            this.driverId = driverId;
            this.desc = desc;
        }
    }

    /**
     * 工作节点启动Executor
     * */
    class LaunchExecutor implements DeployMessage {
        public String appId;
        public int execId;
        public ApplicationDescription appDesc;
        public int cores;
        public int memory;

        public LaunchExecutor(String appId, int execId, ApplicationDescription appDesc,
                              int cores, int memory) {
            this.appId = appId;
            this.execId = execId;
            this.appDesc = appDesc;
            this.cores = cores;
            this.memory = memory;
        }
    }

    class ApplicationFinished implements DeployMessage {
        String appId;

        public ApplicationFinished(String appId) {
            this.appId = appId;
        }
    }

    /**
     * Driver状态查询消息
     * */
    class RequestDriverStatus implements DeployMessage {
        public String driverId;

        public RequestDriverStatus(String driverId) {
            this.driverId = driverId;
        }
    }


    /**
     * 向工作节点发送KillExecutor消息
     * */
    class KillExecutor implements DeployMessage {
        public String appId;
        public int execId;

        public KillExecutor(String appId, int execId) {
            this.appId = appId;
            this.execId = execId;
        }
    }



    /*******************************Spark App注册及资源申请(StandaloneAppClient)*****************************/
    class RegisterApplication implements DeployMessage {
        public ApplicationDescription appDescription;
        public RpcEndpointRef driver;

        public RegisterApplication(ApplicationDescription appDescription, RpcEndpointRef driver) {
            this.appDescription = appDescription;
            this.driver = driver;
        }
    }

    class RegisteredApplication implements DeployMessage {
        public String appId;
        public RpcEndpointRef master;

        public RegisteredApplication(String appId, RpcEndpointRef master) {
            this.appId = appId;
            this.master = master;
        }
    }

    class UnregisterApplication implements DeployMessage {
        public String appId;

        public UnregisterApplication(String appId) {
            this.appId = appId;
        }
    }

    class ApplicationRemoved implements DeployMessage {
        public String message;

        public ApplicationRemoved(String message) {
            this.message = message;
        }
    }

    class StopAppClient implements DeployMessage {}

    class RequestExecutors implements DeployMessage {
        public String appId;
        public int requestedTotal;

        public RequestExecutors(String appId, int requestedTotal) {
            this.appId = appId;
            this.requestedTotal = requestedTotal;
        }
    }

    class ExecutorAdded implements DeployMessage {
        public int execId;
        public String workerId;
        public String host;
        public int cores;
        public int memory;

        public ExecutorAdded(int execId, String workerId, String host, int cores, int memory) {
            this.execId = execId;
            this.workerId = workerId;
            this.host = host;
            this.cores = cores;
            this.memory = memory;
        }
    }

    class ExecutorUpdated implements DeployMessage {
        public int id;
        public ExecutorState state;
        public String message;
        public int exitStatus;
        public boolean workerLost;

        public ExecutorUpdated(int id, ExecutorState state, String message,
                               int exitStatus, boolean workerLost) {
            this.id = id;
            this.state = state;
            this.message = message;
            this.exitStatus = exitStatus;
            this.workerLost = workerLost;
        }
    }

    class KillExecutors implements DeployMessage {
        public String appId;
        public List<String> executorIds;

        public KillExecutors(String appId, List<String> executorIds) {
            this.appId = appId;
            this.executorIds = executorIds;
        }
    }

    class WorkerRemoved implements DeployMessage {
        public String id;
        public String host;
        public String message;

        public WorkerRemoved(String id, String host, String message) {
            this.id = id;
            this.host = host;
            this.message = message;
        }
    }

    class MasterChanged implements DeployMessage {
        public RpcEndpointRef master;
        public String masterWebUiUrl;

        public MasterChanged(RpcEndpointRef master, String masterWebUiUrl) {
            this.master = master;
            this.masterWebUiUrl = masterWebUiUrl;
        }
    }

    class MasterChangeAcknowledged implements DeployMessage {
        public String appId;

        public MasterChangeAcknowledged(String appId) {
            this.appId = appId;
        }
    }
    /************************************************************************************************/
}
