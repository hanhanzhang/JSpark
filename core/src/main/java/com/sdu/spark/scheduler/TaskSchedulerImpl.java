package com.sdu.spark.scheduler;

import com.sdu.spark.SparkContext;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.SchedulableBuilder.*;

/**
 * @author hanhan.zhang
 * */
public class TaskSchedulerImpl implements TaskScheduler {

    public static final String SCHEDULER_MODE_PROPERTY = "spark.scheduler.mode";


    public SparkContext sc;
    private SparkConf conf;
    private int maxTaskFailures;
    private boolean isLocal;


    private SchedulerBackend backend;
    private SchedulingMode schedulingMode;
    private SchedulableBuilder schedulableBuilder;
    private Pool rootPool;

    public TaskSchedulerImpl(SparkContext sc, int maxTaskFailures) {
        this(sc, maxTaskFailures, false);
    }

    public TaskSchedulerImpl(SparkContext sc, int maxTaskFailures, boolean isLocal) {
        this.sc = sc;
        this.conf = this.sc.conf;
        this.maxTaskFailures = maxTaskFailures;
        this.isLocal = isLocal;

        this.schedulingMode = SchedulingMode.withName(this.conf.get(SCHEDULER_MODE_PROPERTY, SchedulingMode.FIFO.name()));
        this.rootPool = new Pool("", schedulingMode, 0, 0);
    }

    public void initialize(SchedulerBackend schedulerBackend) {
        this.backend = schedulerBackend;
        switch (schedulingMode) {
            case FAIR:
                schedulableBuilder = new FairSchedulableBuilder(rootPool, conf);
                schedulableBuilder.buildPools();
                break;
            case FIFO:
                schedulableBuilder = new FIFOSchedulableBuilder(rootPool);
                schedulableBuilder.buildPools();
                break;
            default:
                throw new IllegalArgumentException("Unsupported schedule mode : " + schedulingMode);
        }
    }

    @Override
    public void start() {
        this.backend.start();
    }
}
