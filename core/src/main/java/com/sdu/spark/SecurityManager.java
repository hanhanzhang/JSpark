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
    public static final String SPARK_AUTH_SECRET_CONF = "spark.authenticate.secret";

    private SparkConf conf;
    private byte[] ioEncryptionKey;

    private String secretKey;

    public SecurityManager(SparkConf conf) {
        this(conf, null);
    }

    public SecurityManager(SparkConf conf, byte[] ioEncryptionKey) {
        this.conf = conf;
        this.ioEncryptionKey = ioEncryptionKey;
    }

    public boolean isAuthenticationEnabled() {
        return conf.getBoolean("spark.authenticate", false);
    }

    public boolean isEncryptionEnabled() {
        return conf.getBoolean("spark.network.crypto.enabled", false) ||
                conf.getBoolean("spark.authenticate.enableSaslEncryption", false);
    }

    public String getSaslUser() {
        return "sparkSaslUser";
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
