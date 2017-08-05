package com.sdu.spark;

import com.google.common.collect.Maps;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.rdd.Transaction;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.*;
import com.sdu.spark.scheduler.cluster.StandaloneSchedulerBackend;
import com.sdu.spark.utils.CallSite;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.sdu.spark.SparkApp.TaskSchedulerIsSet;
import static com.sdu.spark.SparkMasterRegex.SPARK_REGEX;
import static com.sdu.spark.utils.Utils.getFutureResult;
import static com.sdu.spark.utils.Utils.isLocalMaster;

/**
 * {@link SparkContext}职责:
 *
 * 1: 创建{@link HeartBeatReceiver}
 *
 *   todo
 *
 * 2: 初始化{@link DAGScheduler}
 *
 *   DAGScheduler根据是否是宽依赖划分Stage, Stage可分为两类: ShuffleMapStage和ResultStage
 *
 *
 *
 * 3: 初始化{@link SchedulerBackend}
 *
 *   SchedulerBackend负责创建Cluster Master的客】、户端(注册SparkApp, 申请Executor资源等)
 *
 *   SchedulerBackend负责启动DriverEndPoint负责监听Executor运行结果
 *
 * 4: 初始化{@link TaskScheduler}
 *
 *   todo
 *
 * Note:
 *
 *  1：Spark概念
 *
 *    1': Job
 *
 *      Spark Transaction Action触发Job生成
 *
 *    2': Stage
 *
 *      Stage是Job的子集, 以宽依赖划分Stage(shuffle操作划分Stage)
 *
 *    3': Task
 *
 *      Task是Stage的子集, 以并行度(partition)来衡量[partition = task]
 *
 *  2: Spark调度
 *
 *    1': State调度[DAGScheduler]
 *
 *    2': Task调度[TaskScheduler]
 *
 *
 * @author hanhan.zhang
 * */
public class SparkContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkContext.class);
    public static final String DRIVER_IDENTIFIER = "driver";
    public AtomicBoolean stopped = new AtomicBoolean(false);

    public SparkConf conf;
    public SparkEnv env;
    public LiveListenerBus listenerBus;
    public SchedulerBackend schedulerBackend;
    public TaskScheduler taskScheduler;
    private DAGScheduler dagScheduler;
    private RpcEndPointRef heartbeatReceiver;
    private List<String> jars;
    private int executorMemory;
    private AtomicInteger nextRddId = new AtomicInteger(0);


    public Map<String, String> executorEnvs = Maps.newHashMap();

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
        taskScheduler = tuple.getLeft();
        schedulerBackend = tuple.getRight();
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

    public String appName() {
        return this.conf.get("spark.app.name");
    }
    public int executorMemory() {
        return this.conf.getInt("spark.executor.memory", 1024);
    }
    private String master() {
        return this.conf.get("spark.master");
    }
    private String deployMode() {
        return this.conf.get("spark.submit.deployMode");
    }

    public void stopInNewThread() {
        throw new UnsupportedOperationException("");
    }


    private static SparkEnv createSparkEnv(SparkConf conf, boolean isLocal, LiveListenerBus listenerBus) {
        int cores = Runtime.getRuntime().availableProcessors();
        return SparkEnv.createDriverEnv(conf, isLocal, listenerBus, cores, null);
    }

    /**
     * 仅仅实现Standalone模式
     *
     * todo: Spark Cluster(local, standalone, mesos, yarn)
     * */
    private Pair<TaskScheduler, SchedulerBackend> createTaskScheduler(SparkContext sc, String master, String deployMode) {
        if (SPARK_REGEX(master)) {
            TaskSchedulerImpl scheduler = new TaskSchedulerImpl(sc);
            SchedulerBackend backend = new StandaloneSchedulerBackend(scheduler, sc, master);
            scheduler.initialize(backend);
            return new ImmutablePair<>(scheduler, backend);
        }
        throw new UnsupportedOperationException("Unsupported spark cluster : " + master);
    }


    /**************************Spark Transaction Action触发Job提交***************************/
    public <T, U> void runJob(RDD<T> rdd,
                              Transaction<Pair<TaskContext, Iterator<T>>, Void> func,
                              List<Integer> partitions,
                              Transaction<Pair<Integer, U>, Void> resultHandler) {

    }

    public CallSite getCallSite() {
        throw new UnsupportedOperationException("");
    }

    public int newRddId(){
        return nextRddId.getAndIncrement();
    }

}
