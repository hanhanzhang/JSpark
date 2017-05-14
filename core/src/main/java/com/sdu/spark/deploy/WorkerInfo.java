package com.sdu.spark.deploy;

import com.sdu.spark.rpc.RpcEndPointRef;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public class WorkerInfo implements Serializable{
    /**
     * 工作节点标识
     * */
    @Getter
    private String workerId;
    /**
     * 工作节点地址
     * */
    @Getter
    private String host;
    /**
     * 工作节点监听端口
     * */
    @Getter
    private int port;
    /**
     * 工作节点CPU核数
     * */
    private int cores;
    /**
     * 工作节点JVM内存
     * */
    private int memory;
    /**
     * 工作节点网络通信引用
     * */
    @Getter
    private RpcEndPointRef endPointRef;

    /**
     * 工作节点已用CPU核数
     * */
    private int coresUsed;
    /**
     * 工作节点已用内存
     * */
    private int memoryUsed;
    /**
     * 工作节点状态
     * */
    @Getter
    private WorkerState state;
    /**
     * 上次心跳时间
     * */
    @Setter
    @Getter
    private long lastHeartbeat = System.currentTimeMillis();

    public WorkerInfo(String workerId, String host, int port, int cores, int memory, RpcEndPointRef endPointRef) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.cores = cores;
        this.memory = memory;
        this.state = WorkerState.ALIVE;
        this.endPointRef = endPointRef;
    }

    /**
     * 可用CPU核数
     * */
    public int freeCores() {
        return cores - coresUsed;
    }

    /**
     * 可用内存数
     * */
    public int freeMemory() {
        return memory - memoryUsed;
    }

    public boolean isAlive() {
        return state == WorkerState.ALIVE;
    }
}
