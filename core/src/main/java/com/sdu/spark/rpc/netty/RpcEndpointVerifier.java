package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.*;
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
    public void onStart() {

    }

    @Override
    public void onEnd() {

    }

    @Override
    public void onStop() {

    }

    @Override
    public void onConnect(RpcAddress rpcAddress) {

    }

    @Override
    public void onDisconnect(RpcAddress rpcAddress) {

    }

    @Override
    public void receive(Object msg) {

    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof CheckExistence) {
            CheckExistence existence = (CheckExistence) msg;
            LOGGER.info("JSparkRpcEndPoint verifier name : {}", existence.name);
            RpcEndPointRef endPointRef = dispatcher.verify(existence.name);
            context.reply(endPointRef);
        }
    }
}
