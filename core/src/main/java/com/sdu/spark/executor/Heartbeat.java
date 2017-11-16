package com.sdu.spark.executor;

import com.sdu.spark.storage.BlockManagerId;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public class Heartbeat implements Serializable {
    public String executorId;
    public BlockManagerId blockManagerId;

    public Heartbeat(String executorId, BlockManagerId blockManagerId) {
        this.executorId = executorId;
        this.blockManagerId = blockManagerId;
    }

    @Override
    public String toString() {
        return "Heartbeat(" +
                "executorId='" + executorId + '\'' +
                ", blockManagerId=" + blockManagerId +
                ')';
    }
}
