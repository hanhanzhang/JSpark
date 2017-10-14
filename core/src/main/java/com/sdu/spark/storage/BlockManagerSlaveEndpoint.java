package com.sdu.spark.storage;

import com.sdu.spark.MapOutputTracker;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.ThreadSafeRpcEndpoint;

/**
 *
 * @author hanhan.zhang
 * */
public class BlockManagerSlaveEndpoint extends ThreadSafeRpcEndpoint {

    private BlockManager blockManager;
    private MapOutputTracker mapOutputTracker;

    public BlockManagerSlaveEndpoint(RpcEnv rpcEnv, BlockManager blockManager,
                                     MapOutputTracker mapOutputTracker) {
        super(rpcEnv);
        this.blockManager = blockManager;
        this.mapOutputTracker = mapOutputTracker;
    }
}
