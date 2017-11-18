package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.RpcEndpoint;
import com.sdu.spark.rpc.RpcEndpointRef;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.netty.OutboxMessage.CheckExistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class RpcEndpointVerifier extends RpcEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcEndpointVerifier.class);

    public static final String NAME = "SparkEndPointVerifier";

    private Dispatcher dispatcher;

    public RpcEndpointVerifier(RpcEnv rpcEnv, Dispatcher dispatcher) {
        super(rpcEnv);
        this.dispatcher = dispatcher;
    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof CheckExistence) {
            CheckExistence existence = (CheckExistence) msg;
            LOGGER.info("verifier RpcEndpoint name {}", existence.name);
            RpcEndpointRef endPointRef = dispatcher.verify(existence.name);
            context.reply(endPointRef);
        }
    }
}
