package com.sdu.spark.network.sasl;

/**
 * 获取SASL验证密码
 *
 * @author hanhan.zhang
 * */
public interface SecretKeyHolder {

    String getSaslUser(String appId);

    String getSecretKey(String appId);

}
