package com.sdu.spark.scheduler;

import com.google.common.collect.Sets;
import com.sdu.spark.ExecutorAllocationClient;
import com.sdu.spark.SparkContext;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.utils.Clock;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author hanhan.zhang
 * */
public class BlacklistTracker {

    private LiveListenerBus listenerBus;
    private SparkConf conf;
    private ExecutorAllocationClient allocationClient;
    private Clock clock;

    public AtomicReference<Set<String>> nodeBlacklist = new AtomicReference<>(Sets.newHashSet());

    public BlacklistTracker(SparkContext sc, ExecutorAllocationClient allocationClient) {
        this(sc.listenerBus, sc.conf, allocationClient, new Clock.SystemClock());
    }

    public BlacklistTracker(LiveListenerBus listenerBus, SparkConf conf,
                            ExecutorAllocationClient allocationClient, Clock clock) {
        this.listenerBus = listenerBus;
        this.conf = conf;
        this.allocationClient = allocationClient;
        this.clock = clock;
    }

    public static boolean isBlacklistEnabled(SparkConf conf) {
        String enabled = conf.get("spark.blacklist.enabled");
        if (StringUtils.isNotEmpty(enabled)) {
            return BooleanUtils.toBoolean(enabled);
        }

        String legacyKey = conf.get("spark.scheduler.executorTaskBlacklistTime");
        if (StringUtils.isNotEmpty(legacyKey) && NumberUtils.toLong(legacyKey, 0L) > 0 ) {
            return true;
        }
        return false;
    }

}
