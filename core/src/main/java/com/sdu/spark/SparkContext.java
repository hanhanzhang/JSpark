package com.sdu.spark;

import com.google.common.collect.Maps;
import com.sdu.spark.broadcast.Broadcast;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.rdd.Transaction;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.*;
import com.sdu.spark.scheduler.cluster.StandaloneSchedulerBackend;
import com.sdu.spark.utils.CallSite;
import com.sdu.spark.utils.scala.Tuple2;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.sdu.spark.SparkApp.TaskSchedulerIsSet;
import static com.sdu.spark.SparkMasterRegex.*;
import static com.sdu.spark.utils.Utils.getFutureResult;
import static com.sdu.spark.utils.Utils.isLocalMaster;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

/**
 * {@link SparkContext}职责:
 *
 * 1: {@link #init()} 初始化组件
 *
 *  1':
 *
 *
 * 1: 创建{@link HeartBeatReceiver}
 *
 *   todo
 *
 * 2: 初始化{@link DAGScheduler}
 *
 *   DAGScheduler根据是否是宽依赖划分Stage, Stage可分为两类: ShuffleMapStage和ResultStage
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

    public static final String SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel";

    private static final Object SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object();
    private static AtomicReference<SparkContext> activeContext = new AtomicReference<>(null);

    public static final String DRIVER_IDENTIFIER = "driver";
    public static final String LEGACY_DRIVER_IDENTIFIER = "<driver>";

    private boolean allowMultipleContexts;
    private long startTime;
    public AtomicBoolean stopped = new AtomicBoolean(false);
    // TODO: CallSite


    public SparkConf conf;
    public SparkEnv env;
    public LiveListenerBus listenerBus;
    public SchedulerBackend schedulerBackend;
    public TaskScheduler taskScheduler;
    private volatile DAGScheduler dagScheduler;
    private RpcEndPointRef heartbeatReceiver;
    private List<String> jars;
    private int executorMemory;

    private String applicationId;
    private String applicationAttemptId;

    private AtomicInteger nextShuffleId = new AtomicInteger(0);
    private AtomicInteger nextRddId = new AtomicInteger(0);


    public Map<String, String> executorEnvs = Maps.newHashMap();

    public SparkContext(SparkConf conf) {
        this.conf = conf;
        init();
    }

    private void init() {
        // SparkContext启动时间
        this.startTime = System.currentTimeMillis();
        // jvm进程中是否允许多个SparkContext
        this.allowMultipleContexts = this.conf.getBoolean("spark.driver.allowMultipleContexts", false);
        SparkContext.markPartiallyConstructed(this, this.allowMultipleContexts);

        if (!this.conf.contains("spark.master")) {
            throw new SparkException("A master URL must be set in your configuration");
        }
        if (!this.conf.contains("spark.app.name")) {
            throw new SparkException("An application name must be set in your configuration");
        }

        LOGGER.info("submit spark application {}", appName());

        String master = master();
        String deployMode = deployMode();
        LOGGER.info("Spark: master = {}, deployMode = {}", master, deployMode);

        // TODO: User Jar

        listenerBus = new LiveListenerBus(conf);

        // TODO: JobListener

        env = createSparkEnv(conf, isLocalMaster(conf), listenerBus);
        SparkEnv.env = env;

        executorMemory = conf.getInt("spark.executor.memory", 1024);
        executorEnvs.put("SPARK_EXECUTOR_MEMORY", String.format("%dm", executorMemory));

        heartbeatReceiver = env.rpcEnv.setRpcEndPointRef(HeartBeatReceiver.ENDPOINT_NAME,
                                                         new HeartBeatReceiver(this));

        Tuple2<TaskScheduler, SchedulerBackend> tuple = createTaskScheduler(this, master, deployMode());
        taskScheduler = tuple._1();
        schedulerBackend = tuple._2();
        dagScheduler = new DAGScheduler(this);

        //
        heartbeatReceiver.ask(new TaskSchedulerIsSet());

        // 启动TaskScheduler
        taskScheduler.start();
        applicationId = taskScheduler.applicationId();
        applicationAttemptId = taskScheduler.applicationAttemptId();

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

    public String applicationId() {
        return applicationId;
    }

    public String applicationAttemptId() {
        return applicationAttemptId;
    }

    public void stopInNewThread() {
        throw new UnsupportedOperationException("");
    }

    private static int convertToInt(String threads) {
        return threads.equals("*") ? Runtime.getRuntime().availableProcessors() : toInt(threads);
    }

    private static int numDriverCores(String master) {
        if (master.equals("local")) {
            return 1;
        } else if (LOCAL_N_REGEX(master)) {
            return convertToInt(LOCAL_N_REGEX_THREAD(master));
        } else if (LOCAL_N_FAILURES_REGEX(master)) {
            return convertToInt(LOCAL_N_FAILURES_REGEX_R(master)[0]);
        } else {
            return 0;
        }
    }

    private static SparkEnv createSparkEnv(SparkConf conf, boolean isLocal, LiveListenerBus listenerBus) {
        int cores = numDriverCores(conf.get("spark.master"));
        return SparkEnv.createDriverEnv(conf, isLocal, listenerBus, cores, null);
    }


    private Tuple2<TaskScheduler, SchedulerBackend> createTaskScheduler(SparkContext sc, String master, String deployMode) {
        if (SPARK_REGEX(master)) {
            TaskSchedulerImpl scheduler = new TaskSchedulerImpl(sc);
            SchedulerBackend backend = new StandaloneSchedulerBackend(scheduler, sc, master);
            scheduler.initialize(backend);
            return new Tuple2<>(scheduler, backend);
        }
        // TODO: Spark Cluster(local, standalone, mesos, yarn)
        throw new UnsupportedOperationException("Unsupported spark cluster : " + master);
    }


    /**************************Spark Transaction Action触发Job提交 ***************************/
    public <T, U> void runJob(RDD<T> rdd,
                              Transaction<Pair<TaskContext, Iterator<T>>, Void> func,
                              List<Integer> partitions,
                              Transaction<Pair<Integer, U>, Void> resultHandler) {

    }

    public CallSite getCallSite() {
        throw new UnsupportedOperationException("");
    }

    public int newShuffleId() {
        return nextShuffleId.getAndIncrement();
    }

    public int newRddId(){
        return nextRddId.getAndIncrement();
    }
    
    public <T> Broadcast<T> broadcast(T value) {
        // TODO: 待实现
        throw new UnsupportedOperationException("");
    }

    private void assertNotStopped() {
        if (stopped.get()) {
            // TODO: Active SparkContext CallSite Message
            throw new IllegalStateException("Cannot call methods on a stopped SparkContext");
        }
    }

    private static void markPartiallyConstructed(SparkContext sc, boolean allowMultipleContexts) {
        synchronized (SPARK_CONTEXT_CONSTRUCTOR_LOCK) {
            assertNoOtherContextIsRunning(sc, allowMultipleContexts);
            activeContext.set(sc);
        }
    }

    private static void assertNoOtherContextIsRunning(SparkContext sc, boolean allowMultipleContexts) {
        synchronized (SPARK_CONTEXT_CONSTRUCTOR_LOCK) {
            SparkContext activeSparkContext = activeContext.get();
            if (activeSparkContext != null && !activeSparkContext.equals(sc)) {
                String errMsg = "Only one SparkContext may be running in this JVM (see SPARK-2243). " +
                                "To ignore this error, set spark.driver.allowMultipleContexts = true. ";
                SparkException exception = new SparkException(errMsg);
                if (allowMultipleContexts) {
                    LOGGER.warn("Multiple running SparkContexts detected in the same JVM!", exception);
                } else {
                    throw exception;
                }
            }
        }

        // TODO: contextBeingConstructed
    }
}
