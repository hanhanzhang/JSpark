package com.sdu.spark.scheduler;

import com.sdu.spark.rpc.SparkConf;

import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public interface SchedulableBuilder {

    Pool rootPool();

    void buildPools();

    void addTaskSetManager(Schedulable manager, Properties properties);

    class FIFOSchedulableBuilder implements SchedulableBuilder {

        Pool rootPool;

        public FIFOSchedulableBuilder(Pool rootPool) {
            this.rootPool = rootPool;
        }

        @Override
        public Pool rootPool() {
            return rootPool;
        }

        @Override
        public void buildPools() {

        }

        @Override
        public void addTaskSetManager(Schedulable manager, Properties properties) {
            rootPool.addSchedulable(manager);
        }
    }

    class FairSchedulableBuilder implements SchedulableBuilder {

        Pool rootPool;
        SparkConf conf;

        public FairSchedulableBuilder(Pool rootPool, SparkConf conf) {
            this.rootPool = rootPool;
            this.conf = conf;
        }

        @Override
        public Pool rootPool() {
            return rootPool;
        }

        @Override
        public void buildPools() {

        }

        @Override
        public void addTaskSetManager(Schedulable manager, Properties properties) {

        }
    }

}
