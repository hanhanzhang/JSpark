package com.sdu.spark.executor;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public class HeartbeatResponse implements Serializable {
    // 是否需要向BlockManager注册BlockManagerId
    public boolean registerBlockManager;

    public HeartbeatResponse(boolean registerBlockManager) {
        this.registerBlockManager = registerBlockManager;
    }
}
