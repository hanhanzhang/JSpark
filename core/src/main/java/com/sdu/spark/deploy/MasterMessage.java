package com.sdu.spark.deploy;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
 * Master节点消息
 *
 * @author hanhan.zhang
 * */
public interface MasterMessage extends Serializable {

    /**
     * Master选主消息
     * */
    class ElectedLeader implements MasterMessage {}

    /**
     * Master恢复消息
     * */
    class CompleteRecovery implements MasterMessage {}

    /**
     * 删除主Master
     * */
    class RevokedLeadership implements MasterMessage {}

    /**
     * 应用恢复消息
     * */
    @AllArgsConstructor
    @Getter
    class BeginRecovery implements MasterMessage {
        ApplicationInfo []storedApps;
        WorkerInfo[] storedWorkers;
    }

    /**
     *
     * */
    class BoundPortsRequest implements MasterMessage {}

    @AllArgsConstructor
    @Getter
    class BoundPortsResponse implements MasterMessage {
        int rpcEndpointPort;
        int webUIPort;
        int restPort;
    }

    /**
     * 检查工作节点心跳消息
     * */
    class CheckForWorkerTimeOut implements MasterMessage {}

    /**
     * 查询Master状态信息
     * */
    class RequestMasterState implements MasterMessage {}

}
