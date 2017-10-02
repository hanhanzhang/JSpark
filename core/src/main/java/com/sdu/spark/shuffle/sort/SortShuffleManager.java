package com.sdu.spark.shuffle.sort;

import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.shuffle.ShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class SortShuffleManager implements ShuffleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortShuffleManager.class);

    private SparkConf conf;

    public SortShuffleManager(SparkConf conf) {
        this.conf = conf;
    }

    @Override
    public void stop() {

    }
}
