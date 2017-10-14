package com.sdu.spark.deploy;

import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEnv;

/**
 * todo待实现
 *
 * @author hanhan.zhang
 * */
public class Client {

    public class ClientEndpoint extends RpcEndPoint {
        public ClientEndpoint(RpcEnv rpcEnv) {
            super(rpcEnv);
        }
    }

}
