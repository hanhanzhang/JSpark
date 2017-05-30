package com.sdu.spark.rpc.netty;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.JSparkConfig;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.RpcEnvConfig;
import com.sdu.spark.rpc.RpcEnvFactory;
import com.sdu.spark.serializer.JavaSerializerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class NettyRpcEnvFactory implements RpcEnvFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyRpcEnvFactory.class);

    @Override
    public RpcEnv create(RpcEnvConfig conf) {
        JSparkConfig sparkConfig = conf.conf;
        SecurityManager securityManager = new SecurityManager(sparkConfig);

        // Java序列化对象
        JavaSerializerInstance serializerInstance = new JavaSerializerInstance(sparkConfig.getCountReset(),
                Thread.currentThread().getContextClassLoader());

        NettyRpcEnv rpcEnv = new NettyRpcEnv(sparkConfig, conf.bindAddress, serializerInstance, securityManager);
        if (!conf.clientModel) {
            assert conf.port == 0 || (conf.port >= 1024 && conf.port < 65536) :
                    "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.";
            rpcEnv.startServer(conf.bindAddress, conf.port);
            int actualPort = rpcEnv.address().port;
            LOGGER.info("JSpark start netty rpc env on address {}:{}", conf.bindAddress, actualPort);
        }
        return rpcEnv;
    }

}
