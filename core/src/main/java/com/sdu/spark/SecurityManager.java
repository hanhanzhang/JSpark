package com.sdu.spark;

import com.sdu.spark.network.sasl.SecretKeyHolder;
import com.sdu.spark.rpc.SparkConf;

/**
 * JSpark权限管理
 *
 * @author hanhan.zhang
 * */
public class SecurityManager implements SecretKeyHolder {

    public static final String ENV_AUTH_SECRET = "_SPARK_AUTH_SECRET";

    private SparkConf conf;

    private String secretKey;

    public SecurityManager(SparkConf conf) {
        this.conf = conf;
    }

    public boolean isAuthenticationEnabled() {
        return conf.getBoolean("spark.authenticate", true);
    }

    @Override
    public String getSaslUser(String appId) {
        return null;
    }

    @Override
    public String getSecretKey(String appId) {
        return getSecretKey();
    }

    public String getSecretKey() {
        return secretKey;
    }
}
