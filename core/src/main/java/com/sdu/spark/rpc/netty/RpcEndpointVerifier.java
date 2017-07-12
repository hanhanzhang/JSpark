package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.netty.OutboxMessage.CheckExistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class RpcEndpointVerifier extends RpcEndPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcEndpointVerifier.class);

    public static final String NAME = "SparkEndPointVerifier";

    private RpcEnv rpcEnv;

    private Dispatcher dispatcher;

    public RpcEndpointVerifier(RpcEnv rpcEnv, Dispatcher dispatcher) {
        this.rpcEnv = rpcEnv;
        this.dispatcher = dispatcher;
    }

    @Override
    public RpcEndPointRef self() {
        return rpcEnv.setRpcEndPointRef(NAME, this);
    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof CheckExistence) {
            CheckExistence existence = (CheckExistence) msg;
            LOGGER.info("SparkRpcEndPoint verifier name : {}", existence.name);
            RpcEndPointRef endPointRef = dispatcher.verify(existence.name);
            context.reply(endPointRef);
        }
    }
}
