package com.sdu.spark.executor;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.rpc.*;
import com.sdu.spark.scheduler.TaskDescription;
import com.sdu.spark.scheduler.TaskState;
import com.sdu.spark.scheduler.cluster.CoarseGrainedClusterMessage.*;
import com.sdu.spark.scheduler.cluster.CoarseGrainedClusterMessage.Shutdown;
import com.sdu.spark.serializer.SerializerInstance;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.sdu.spark.utils.Utils.getFutureResult;

/**
 * {@link CoarseGrainedExecutorBackend}两个重要属性:
 *
 *  1: Driver RpcEndPoint引用负责与Spark Driver通信
 *
 *  2: Executor(运行在CoarseGrainedExecutorBackend进程中)负责执行Spark任务并将运行完结果返回给Driver
 *
 * @author hanhan.zhang
 * */
public class CoarseGrainedExecutorBackend extends RpcEndPoint implements ExecutorBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoarseGrainedExecutorBackend.class);

    public RpcEnv rpcEnv;
    public String driverUrl;
    public String executorId;
    public String hostname;
    public int cores;
    public SparkEnv env;
    private AtomicBoolean stopping = new AtomicBoolean(false);

    private Executor executor;
    private RpcEndPointRef driver;
    private SerializerInstance ser;

    public CoarseGrainedExecutorBackend(RpcEnv rpcEnv, String driverUrl, String executorId, String hostname,
                                        int cores, SparkEnv env) {
        this.rpcEnv = rpcEnv;
        this.driverUrl = driverUrl;
        this.executorId = executorId;
        this.hostname = hostname;
        this.cores = cores;
        this.env = env;
        this.ser = this.env.serializer.newInstance();
    }

    @Override
    public RpcEndPointRef self() {
        return rpcEnv.endPointRef(this);
    }

    @Override
    public void onStart() {
        LOGGER.info("ExecutorBackend(execId = {})尝试连接Driver(address = {})", executorId, driverUrl);
        driver = this.rpcEnv.setupEndpointRefByURI(driverUrl);
        if (driver == null) {
            String reason = String.format("ExecutorBackend(execId = %s)无法连接Driver(address = %s)", executorId, driverUrl);
            exitExecutor(1, reason, null, false);
        }
        LOGGER.info("ExecutorBackend(execId = {})尝试向Driver(address = {})注册Executor", executorId, driver.address());
        Future<?> future = driver.ask(new RegisterExecutor(executorId, self(), hostname, cores, Collections.emptyMap()));
        boolean success = getFutureResult(future);
        if (success) {
            LOGGER.info("ExecutorBackend(execId = {})向Driver(address = {})注册Executor成功", executorId, driver.address());
        } else {
            LOGGER.info("ExecutorBackend(execId = {})向Driver(address = {})注册Executor失败", executorId, driver.address());
        }
    }

    @Override
    public void receive(Object msg) {
        if (msg instanceof RegisteredExecutor) {
            LOGGER.info("Driver注册Executor(execId = {})成功", executorId);
            executor = new Executor(executorId, env, false);
        } else if (msg instanceof RegisterExecutorFailed) {
            RegisterExecutorFailed executorFailed = (RegisterExecutorFailed) msg;
            exitExecutor(1, String.format("Executor注册失败: %s", executorFailed.message), null);
        } else if (msg instanceof LaunchTask) {
            if (executor == null) {
                exitExecutor(1, "接收到LaunchTask命令但由于Executor=NULL而退出", null);
            } else {
                LaunchTask task = (LaunchTask) msg;
                try {
                    TaskDescription taskDesc = TaskDescription.decode(task.taskData);
                    executor.launchTask(this, taskDesc);
                } catch (IOException e) {
                    LOGGER.error("序列化Spark任务异常", e);
                }
            }
        } else if (msg instanceof KillTask) {
            if (executor == null) {
                exitExecutor(1, "接收到KillTask命令但由于Executor=NULL而退出", null);
            } else {
                KillTask killTask = (KillTask) msg;
                executor.killTask(killTask.taskId, killTask.interruptThread, killTask.reason);
            }
        } else if (msg instanceof StopExecutor) {
            stopping.set(true);
            LOGGER.info("接收到Driver命令: Shutdown");
            self().send(new Shutdown());
        } else if (msg instanceof Shutdown) {
            stopping.set(true);
            new Thread("CoarseGrainedExecutorBackend-stop-executor") {
                @Override
                public void run() {
                    // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
                    // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
                    // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
                    // Therefore, we put this line in a new thread.
                    try {
                        executor.stop();
                    } catch (InterruptedException e) {
                        LOGGER.error("关闭Executor异常", e);
                    }
                }
            }.start();
        }
    }

    @Override
    public void statusUpdate(long taskId, TaskState state, ByteBuffer data) {

    }

    private void exitExecutor(int code, String reason, Throwable throwable) {
        exitExecutor(code, reason, throwable, true);
    }

    private void exitExecutor(int code, String reason, Throwable throwable, boolean notifyDriver) {
        String message = String.format("Executor退出原因: %s", reason);
        if (throwable != null) {
            LOGGER.error(message, throwable);
        } else {
            LOGGER.error(message);
        }
        if (notifyDriver && driver != null) {
            Future<?> future = driver.ask(new RemoveExecutor(executorId, reason));
            boolean success = getFutureResult(future);
            if (!success) {
                LOGGER.error("Driver下线Executor(execId = {})失败", executorId);
            }
        }
        System.exit(code);
    }

    private static void run(String driverUrl, String executorId, String hostname, int cores, String appId) {
        // create RpcEnv
        SparkConf conf = new SparkConf();
        RpcEnv rpcEnv = RpcEnv.create(hostname, -1, conf, new SecurityManager(conf), true);

        RpcEndPointRef driverRef = rpcEnv.setupEndpointRefByURI(driverUrl);
        try {
            SparkAppConfig cfg = (SparkAppConfig) driverRef.askSync(new RetrieveSparkAppConfig(), Integer.MAX_VALUE);
            cfg.sparkProperties.put("spark.app.id", appId);
            rpcEnv.shutdown();

            SparkConf driverConf = new SparkConf();
            Iterator<String> iterator = cfg.sparkProperties.stringPropertyNames().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                String value = cfg.sparkProperties.getProperty(key);
                // this is required for SSL in standalone mode
                if (SparkConf.isExecutorStartupConf(key)) {
                    driverConf.setIfMissing(key, value);
                } else {
                    driverConf.set(key, value);
                }
            }

            SparkEnv env = SparkEnv.createExecutorEnv(
                    driverConf, executorId, hostname, cores, cfg.ioEncryptionKey, false);

            env.rpcEnv.setRpcEndPointRef("Executor", new CoarseGrainedExecutorBackend(env.rpcEnv, driverUrl,
                                                                                      executorId, hostname,
                                                                                      cores, env));
            env.rpcEnv.awaitTermination();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) {
        String driverUrl = null, executorId = null, hostname = null, appId = null;
        int cores = 0;
        if (args != null && args.length > 0) {
            for (String arg : args) {
                String []p = arg.split(" ");
                switch (p[0]) {
                    case "--driver":
                        driverUrl = p[1];
                        break;
                    case "--executor":
                        executorId = p[1];
                        break;
                    case "--host":
                        hostname = p[1];
                        break;
                    case "--cores":
                        cores = NumberUtils.toInt(p[1]);
                        break;
                    case "--appId":
                        appId = p[1];
                        break;
                }
            }

            run(driverUrl, executorId, hostname, cores, appId);
        }
    }
}
