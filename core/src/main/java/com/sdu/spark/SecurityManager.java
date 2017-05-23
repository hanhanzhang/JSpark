package com.sdu.spark;

import com.sdu.spark.network.sasl.SecretKeyHolder;
import com.sdu.spark.rpc.JSparkConfig;

/**
 * JSpark权限管理
 *
 * @author hanhan.zhang
 * */
public class SecurityManager implements SecretKeyHolder {

    private JSparkConfig config;

    public SecurityManager(JSparkConfig config) {
        this.config = config;
    }

    public boolean isAuthenticationEnabled() {
        return config.isAuthenticationEnabled();
    }

    @Override
    public String getSaslUser(String appId) {
        return null;
    }

    @Override
    public String getSecretKey(String appId) {
        return null;
    }
}
