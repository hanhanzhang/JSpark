package com.sdu.spark.network.netty;

import com.sdu.spark.network.utils.ConfigProvider;
import com.sdu.spark.network.utils.TransportConf;
import com.sdu.spark.rpc.SparkConf;

import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class SparkTransportConf {

    private static final int MAX_DEFAULT_NETTY_THREADS = 8;

    public static TransportConf fromSparkConf(SparkConf conf, String module, int numUsableCores) {

        // Specify thread configuration based on our JVM's allocation of cores (rather than necessarily
        // assuming we have all the machine's cores).
        // NB: Only set if serverThreads/clientThreads not already set.
        int numThreads = defaultNumThreads(numUsableCores);
        conf.setIfMissing(String.format("spark.%s.io.serverThreads", module), String.valueOf(numThreads));
        conf.setIfMissing(String.format("spark.%s.io.clientThreads", module), String.valueOf(numThreads));

        return new TransportConf(module, new ConfigProvider() {
            @Override
            public String get(String name) {
                return conf.get(name);
            }

            @Override
            public String get(String name, String defaultValue) {
                return conf.get(name, defaultValue);
            }

            @Override
            public Map<String, String> getAll() {
                return conf.getAll();
            }
        });

    }

    private static int defaultNumThreads(int numUsableCores) {
        int availableCores = numUsableCores > 0 ? numUsableCores : Runtime.getRuntime().availableProcessors();
        return Math.min(availableCores, MAX_DEFAULT_NETTY_THREADS);
    }
}
