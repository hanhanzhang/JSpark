package com.sdu.spark.scheduler;

import com.sdu.spark.MapOutputTrackerMaster;
import com.sdu.spark.SparkContext;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.storage.BlockManagerMaster;
import com.sdu.spark.utils.Clock;
import com.sdu.spark.utils.Clock.*;

/**
 * @author hanhan.zhang
 * */
public class DAGScheduler {

    private SparkContext sc;
    private TaskScheduler taskScheduler;
    private LiveListenerBus listenerBus;
    private MapOutputTrackerMaster mapOutputTracker;
    private BlockManagerMaster blockManagerMaster;
    private SparkEnv env;
    private Clock clock;


    public DAGScheduler(SparkContext sc, TaskScheduler taskScheduler) {
        this(sc, taskScheduler, sc.listenerBus, (MapOutputTrackerMaster) sc.env.mapOutputTracker, sc.env.blockManager.master, sc.env);
    }

    public DAGScheduler(SparkContext sc) {
        this(sc, sc.taskScheduler);
    }

    public DAGScheduler(SparkContext sc, TaskScheduler taskScheduler, LiveListenerBus listenerBus,
                        MapOutputTrackerMaster mapOutputTracker, BlockManagerMaster blockManagerMaster,
                        SparkEnv env) {
        this(sc, taskScheduler, listenerBus, mapOutputTracker, blockManagerMaster, env, new SystemClock());
    }

    public DAGScheduler(SparkContext sc, TaskScheduler taskScheduler, LiveListenerBus listenerBus,
                        MapOutputTrackerMaster mapOutputTracker, BlockManagerMaster blockManagerMaster,
                        SparkEnv env, Clock clock) {
        this.sc = sc;
        this.taskScheduler = taskScheduler;
        this.listenerBus = listenerBus;
        this.mapOutputTracker = mapOutputTracker;
        this.blockManagerMaster = blockManagerMaster;
        this.env = env;
        this.clock = clock;
    }
}
