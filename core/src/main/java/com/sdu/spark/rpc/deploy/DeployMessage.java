package com.sdu.spark.rpc.deploy;

import com.sdu.spark.rpc.RpcEndPointRef;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
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
     * 工作节点注册响应消息
     * */
    @AllArgsConstructor
    @Getter
    class RegisteredWorker implements DeployMessage {
        private RpcEndPointRef master;
    }

    /**
     * 工作节点注册失败消息
     * */
    @AllArgsConstructor
    @Getter
    class RegisterWorkerFailed implements DeployMessage {
        private String message;
    }

    /**
     * 应用注册消息
     * */
    class RegisterApplication implements DeployMessage {

    }

}
