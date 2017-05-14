package com.sdu.spark.deploy;

import com.sdu.spark.rpc.RpcEndPointRef;
import lombok.AllArgsConstructor;
import lombok.Getter;

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
    @AllArgsConstructor
    @Getter
    class WorkerHeartbeat implements DeployMessage {
        private String workerId;
        private RpcEndPointRef worker;
    }

    class SendHeartbeat implements DeployMessage {}

    /**
     * 工作节点注册消息
     * */
    @AllArgsConstructor
    @Getter
    class RegisterWorker implements DeployMessage {
        private String workerId;
        private String host;
        private int port;
        private int cores;
        private int memory;
        private RpcEndPointRef worker;
    }

    /**
     * 工作节点注册响应父类
     * */
    interface RegisteredWorkerResponse {}

    /**
     * 工作节点注册响应消息
     * */
    @AllArgsConstructor
    @Getter
    class RegisteredWorker implements DeployMessage, RegisteredWorkerResponse {
        private RpcEndPointRef master;
    }

    /**
     * 工作节点注册失败消息
     * */
    @AllArgsConstructor
    @Getter
    class RegisterWorkerFailed implements DeployMessage, RegisteredWorkerResponse {
        private String message;
    }

    /**
     * 工作节点状态消息
     * */
    @AllArgsConstructor
    @Getter
    class WorkerLatestState implements DeployMessage {
        private String workerId;
        private List<ExecutorDescription> executors;
        private List<String> driverIds;
    }

    /**
     * 工作节点资源调度响应消息
     * */
    @AllArgsConstructor
    @Getter
    class WorkerSchedulerStateResponse implements DeployMessage {
        private String workerId;
        private List<ExecutorDescription> executors;
        private List<String> driverIds;
    }

    /**
     * 应用注册消息
     * */
    class RegisterApplication implements DeployMessage {

    }

    /**
     * Worker重连消息
     * */
    @AllArgsConstructor
    @Getter
    class ReconnectWorker implements DeployMessage {
        private RpcEndPointRef master;
    }

    /**
     * Executor变更消息
     * */
    @AllArgsConstructor
    @Getter
    class ExecutorStateChanged implements DeployMessage {
        private String executorId;
        private String appId;
        private ExecutorState state;
        private String message;
        private int exitStatus;
    }

}
