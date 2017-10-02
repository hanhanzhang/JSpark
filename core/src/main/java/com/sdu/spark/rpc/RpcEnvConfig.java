package com.sdu.spark.rpc;

import com.sdu.spark.SecurityManager;
import lombok.AllArgsConstructor;

/**
 * @author hanhan.zhang
 * */
public class RpcEnvConfig {

    public SparkConf conf;

    public String name;

    public String bindAddress;

    public String advertiseAddress;

    public int port;

    public SecurityManager securityManager;

    public int numUsableCores;

    public boolean clientModel;

    public RpcEnvConfig(SparkConf conf, String name, String bindAddress, String advertiseAddress, int port,
                        SecurityManager securityManager, int numUsableCores, boolean clientModel) {
        this.conf = conf;
        this.name = name;
        this.bindAddress = bindAddress;
        this.advertiseAddress = advertiseAddress;
        this.port = port;
        this.securityManager = securityManager;
        this.numUsableCores = numUsableCores;
        this.clientModel = clientModel;
    }
}
