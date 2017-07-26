package com.sdu.spark;

import com.google.common.collect.Maps;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.*;
import com.sdu.spark.scheduler.local.LocalSchedulerBackend;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.sdu.spark.SparkApp.*;

import java.util.List;
import java.util.Map;

import static com.sdu.spark.SparkMasterRegex.*;

import static com.sdu.spark.utils.Utils.getFutureResult;
import static com.sdu.spark.utils.Utils.isLocalMaster;

/**
 * SparkContext
 *
 * @author hanhan.zhang
 * */
public class SparkContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkContext.class);
    public static final String DRIVER_IDENTIFIER = "driver";

    public SparkConf conf;
    public SparkEnv env;
    public LiveListenerBus listenerBus;
    public SchedulerBackend schedulerBackend;
    public TaskScheduler taskScheduler;
    private DAGScheduler dagScheduler;
    private RpcEndPointRef heartbeatReceiver;
    private List<String> jars;
    private int executorMemory;

    private Map<String, String> executorEnvs = Maps.newHashMap();

    public SparkContext(SparkConf conf) {
        this.conf = conf;
        init();
    }

    private void init() {
        if (!this.conf.contains("spark.master")) {
            throw new IllegalStateException("A master URL must be set in your configuration");
        }
        if (!this.conf.contains("spark.app.name")) {
            throw new IllegalStateException("An application name must be set in your configuration");
        }

        LOGGER.info("提交Spark应用: {}", appName());

        String master = master();
        String deployMode = deployMode();
        LOGGER.info("Spark: master = {}, deployMode = {}", master, deployMode);

        // TODO: User Jar

        listenerBus = new LiveListenerBus(conf);

        // TODO: JobListener

        env = createSparkEnv(conf, isLocalMaster(conf), listenerBus);
        SparkEnv.env = env;

        executorMemory = conf.getInt("spark.executor.memory", 1024);
        executorEnvs.put("SPARK_EXECUTOR_MEMORY", String.format("%sm", executorMemory));

        heartbeatReceiver = env.rpcEnv.setRpcEndPointRef(HeartBeatReceiver.ENDPOINT_NAME,
                                                         new HeartBeatReceiver(this));

        Pair<TaskScheduler, SchedulerBackend> tuple = createTaskScheduler(this, master, deployMode());
        taskScheduler = tuple.getKey();
        schedulerBackend = tuple.getValue();
        dagScheduler = new DAGScheduler(this);
        boolean isSet = getFutureResult(heartbeatReceiver.ask(new TaskSchedulerIsSet()));
        if (isSet) {
            LOGGER.info("HeartbeatReceiver初始TaskScheduler成功");
        } else {
            LOGGER.info("HeartbeatReceiver初始TaskScheduler失败");
        }

        // 启动TaskScheduler
        taskScheduler.start();

    }

    private String appName() {
        return this.conf.get("spark.app.name");
    }
    private String master() {
        return this.conf.get("spark.master");
    }
    private String deployMode() {
        return this.conf.get("spark.submit.deployMode");
    }


    private static SparkEnv createSparkEnv(SparkConf conf, boolean isLocal, LiveListenerBus listenerBus) {
        int cores = Runtime.getRuntime().availableProcessors();
        return SparkEnv.createDriverEnv(conf, isLocal, listenerBus, cores, null);
    }

    private Pair<TaskScheduler, SchedulerBackend> createTaskScheduler(SparkContext sc, String master, String deployMode) {
        Pair<TaskScheduler, SchedulerBackend> tuple = null;
        // Task执行失败重试次数
        int MAX_LOCAL_TASK_FAILURES = 1;
        if (master.equals("local")) {
            TaskSchedulerImpl scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, true);
            LocalSchedulerBackend schedulerBackend = new LocalSchedulerBackend(conf, scheduler, 1);
            scheduler.initialize(schedulerBackend);

            tuple = new ImmutablePair<>(scheduler, schedulerBackend);
        } else if (LOCAL_N_REGEX(master)) {
            int cpuCount = LOCAL_N_REGEX_THREAD(master);

            TaskSchedulerImpl scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, true);
            LocalSchedulerBackend schedulerBackend = new LocalSchedulerBackend(conf, scheduler, cpuCount);
            scheduler.initialize(schedulerBackend);

            tuple = new ImmutablePair<>(scheduler, schedulerBackend);
        } else if (LOCAL_N_FAILURES_REGEX(master)) {
            int[] r = LOCAL_N_FAILURES_REGEX_R(master);
            int cpuCount = r[0];
            int maxFailures = r[1];

            TaskSchedulerImpl scheduler = new TaskSchedulerImpl(sc, maxFailures, true);
            LocalSchedulerBackend schedulerBackend = new LocalSchedulerBackend(conf, scheduler, cpuCount);
            scheduler.initialize(schedulerBackend);

            tuple = new ImmutablePair<>(scheduler, schedulerBackend);
        } else if (SPARK_REGEX(master)) {

        } else if (LOCAL_CLUSTER_REGEX(master)) {

        }
        return tuple;
    }

}
