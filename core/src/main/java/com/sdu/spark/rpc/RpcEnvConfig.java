package com.sdu.spark.rpc;

import com.sdu.spark.SecurityManager;
import lombok.AllArgsConstructor;

/**
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class RpcEnvConfig {

    public JSparkConfig conf;

    public String bindAddress;

    public int port;

    public SecurityManager securityManager;

    public boolean clientModel;

}
