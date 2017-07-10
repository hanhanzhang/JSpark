package com.sdu.spark.rpc;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.sdu.spark.utils.Utils.timeStringAs;

/**
 *
 * @author hanhan.zhang
 * */
public class SparkConf implements Serializable {

    private Map<String, String> settings = Maps.newConcurrentMap();

    public void set(String key, String value) {
        settings.put(key, value);
    }

    public String get(String key, String defaultValue) {
        return settings.getOrDefault(key, defaultValue);
    }

    public String get(String key) {
        return settings.get(key);
    }

    public long getLong(String key, long defaultValue) {
        String value = settings.get(key);
        return NumberUtils.toLong(value, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        String value = settings.get(key);
        return NumberUtils.toInt(value, defaultValue);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = settings.get(key);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }
        return Boolean.valueOf(key);
    }

    public long getTimeAsMs(String key, String defaultValue) {
        return timeStringAs(get(key, defaultValue), TimeUnit.MILLISECONDS);
    }

    public String getAppId() {
        return get("spark.app.id");
    }
}
