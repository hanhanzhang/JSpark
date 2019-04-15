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
     * RpcEndPoint绑定端口请求
     * */
    class BoundPortsRequest implements MasterMessage {}

    /**
     * RpcEndPoint端口绑定响应
     * */
    class BoundPortsResponse implements MasterMessage {
        int rpcEndpointPort;
        int restPort;

        public BoundPortsResponse(int rpcEndpointPort, int restPort) {
            this.rpcEndpointPort = rpcEndpointPort;
            this.restPort = restPort;
        }
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
