package com.sdu.spark.deploy;

import com.sdu.spark.rpc.RpcEndPointRef;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class WorkerInfo implements Serializable{
    /**
     * 工作节点标识
     * */
    public String workerId;
    /**
     * 工作节点地址
     * */
    public String host;
    /**
     * 工作节点监听端口
     * */
    public int port;
    /**
     * 工作节点CPU核数
     * */
    public int cores;
    /**
     * 工作节点JVM内存
     * */
    public long memory;
    /**
     * 工作节点网络通信引用
     * */
    public RpcEndPointRef endPointRef;



    /*****************************不需要序列化*********************************/
    /**
     * 工作节点已用CPU核数
     * */
    public transient int coresUsed;
    /**
     * 工作节点已用内存
     * */
    public transient long memoryUsed;
    /**
     * 工作节点状态
     * */
    public transient WorkerState state;
    /**
     * 上次心跳时间
     * */
    public  long lastHeartbeat = System.currentTimeMillis();
    public transient Map<String, ExecutorDesc> executors;
    public transient Map<String, DriverInfo> drivers;

    public WorkerInfo(String workerId, String host, int port, int cores, long memory, RpcEndPointRef endPointRef) {
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
    public long freeMemory() {
        return memory - memoryUsed;
    }

    public boolean isAlive() {
        return state == WorkerState.ALIVE;
    }

    public void addDriver(DriverInfo driver) {
        drivers.put(driver.id, driver);
        memoryUsed += driver.desc.mem;
        coresUsed += driver.desc.cores;
    }

    public void addExecutor(ExecutorDesc desc) {
        executors.put(desc.fullId(), desc);
        coresUsed += desc.cores;
        memoryUsed += desc.memory;
    }

    public void removeExecutor(ExecutorDesc exec) {
        if (executors.containsKey(exec.fullId())) {
            executors.remove(exec.fullId());
            coresUsed -= exec.cores;
            memoryUsed -= exec.memory;
        }
    }
}
