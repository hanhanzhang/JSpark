package com.sdu.spark.deploy;

import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.ThreadSafeRpcEndpoint;

/**
 * todo待实现
 *
 * @author hanhan.zhang
 * */
public class Client {

    public class ClientEndpoint extends ThreadSafeRpcEndpoint {
        public ClientEndpoint(RpcEnv rpcEnv) {
            super(rpcEnv);
        }
    }

}
