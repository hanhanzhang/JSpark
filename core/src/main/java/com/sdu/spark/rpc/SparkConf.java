package com.sdu.spark.rpc;

import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.sdu.spark.utils.Utils.timeStringAs;

/**
 *
 * @author hanhan.zhang
 * */
@Getter
@Builder
public class SparkConf implements Serializable {

    /**
     * Rpc事件分发线程数
     * */
    private int dispatcherThreads;
    /**
     * Rpc连接线程数
     * */
    private int rpcConnectThreads;
    /**
     * Rpc消息投递线程数
     * */
    private int deliverThreads;

    /**
     * Master定时check工作节点时间间隔
     * */
    private int checkWorkerTimeout;
    /**
     * Worker节点心跳超时时间
     * */
    private int workerTimeout;
    /**
     * 挂掉Worker持续存在的最大心跳次数
     * */
    private int deadWorkerPersistenceTimes;
    /**
     * 向Master注册重试次数
     * */
    private int maxRetryConnectTimes;
    /**
     * RpcServer是否验证
     * */
    private boolean authenticationEnabled;

    /**
     * Rpc ObjectOutStream重置阈值
     * */
    private int countReset = 100;

    private Map<String, String> settings = Maps.newConcurrentMap();

    public String get(String key, String defaultValue) {
        return settings.getOrDefault(key, defaultValue);
    }

    public String get(String key) {
        return settings.get(key);
    }

    public int getInt(String key, int defaultValue) {
        String value = settings.get(key);
        return NumberUtils.toInt(value, defaultValue);
    }

    public long getTimeAsMs(String key, String defaultValue) {
        return timeStringAs(get(key, defaultValue), TimeUnit.MILLISECONDS);
    }

    public String getAppId() {
        return get("spark.app.id");
    }
}
