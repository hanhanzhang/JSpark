package com.sdu.spark.executor;

import com.sdu.spark.executor.ExecutorExitCode.*;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.deploy.worker.WorkerWatcher;
import com.sdu.spark.rpc.*;
import com.sdu.spark.scheduler.TaskDescription;
import com.sdu.spark.scheduler.TaskState;
import com.sdu.spark.scheduler.cluster.CoarseGrainedClusterMessage.*;
import com.sdu.spark.scheduler.cluster.CoarseGrainedClusterMessage.Shutdown;
import com.sdu.spark.serializer.SerializerInstance;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 * {@link CoarseGrainedExecutorBackend}职责:
 *
 * 1: CoarseGrainedExecutorBackend启动流程
 *
 *    Spark Submit Job时向Master申请Executor(StandaloneSchedulerBackend.start())资源, Master收到Executor资源申请请求
 *
 *    选择满足资源需求的Worker节点启动CoarseGrainedExecutorBackend(ExecutorRunner)
 *
 * 2: CoarseGrainedExecutorBackend与Driver通信
 *
 *    1': LaunchTask, 其调用链:
 *
 *      TaskScheduler.submitTasks()[Driver]
 *        |
 *        +---> CoarseGrainedSchedulerBackend.reviveOffers()[Driver]
 *        |
 *        +---> CoarseGrainedSchedulerBackend.DriverEndpoint.launchTasks()[Driver]
 *                |
 *                +---> CoarseGrainedExecutorBackend.LaunchTask()[Executor]
 *                         |
 *                         +---> Executor.launchTask()
 *
 *    2': StatusUpdate: 作业运行状态上报Driver, 其调用链:
 *
 *      Executor.TaskRunner.run()[Executor]
 *        |
 *        +---> CoarseGrainedExecutorBackend.statusUpdate()[Executor]
 *                |
 *                +---> CoarseGrainedSchedulerBackend.DriverEndpoint.receive(StatusUpdate)[Driver]
 *                         |
 *                         +---> CoarseGrainedSchedulerBackend.StatusUpdate()
 *
 * @author hanhan.zhang
 * */
public class CoarseGrainedExecutorBackend extends ThreadSafeRpcEndpoint implements ExecutorBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoarseGrainedExecutorBackend.class);

    public String driverUrl;
    public String executorId;
    public String hostname;
    public int cores;
    private URL[] userClassPath;
    public SparkEnv env;
    private AtomicBoolean stopping = new AtomicBoolean(false);

    private Executor executor;
    private volatile RpcEndpointRef driver;
    private SerializerInstance ser;

    public CoarseGrainedExecutorBackend(RpcEnv rpcEnv,
                                        String driverUrl,
                                        String executorId,
                                        String hostname,
                                        int cores,
                                        URL[] userClassPath,
                                        SparkEnv env) {
        super(rpcEnv);
        this.driverUrl = driverUrl;
        this.executorId = executorId;
        this.hostname = hostname;
        this.cores = cores;
        this.userClassPath = userClassPath;
        this.env = env;
        this.ser = this.env.closureSerializer.newInstance();
    }

    @Override
    public void onStart() {
        LOGGER.info("Connecting to driver: {}", driverUrl);
        driver = this.rpcEnv.setupEndpointRefByURI(driverUrl);
        if (driver == null) {
            exitExecutor(1, format("Cannot register with driver: %s", driverUrl), null, false);
        }

        // 注册Executor资源
        driver.ask(new RegisterExecutor(executorId,
                                        self(),
                                        hostname,
                                        cores,
                                        extractLogUrls()));
    }
    
    private Map<String, String> extractLogUrls() {
        // TODO: 日志上报地址
        return Collections.emptyMap();
    }

    @Override
    public void receive(Object msg) {
        if (msg instanceof RegisteredExecutor) {
            LOGGER.info("Successfully registered with driver");
            try {
                executor = new Executor(executorId, hostname, env, userClassPath);
            } catch (Exception e) {
                exitExecutor(1, "Unable to create executor due to {}" + e.getMessage(), e);
            }
        } else if (msg instanceof RegisterExecutorFailed) {
            RegisterExecutorFailed failed = (RegisterExecutorFailed) msg;
            exitExecutor(1, format("Slave registration failed: %s", failed.message), null);
        } else if (msg instanceof LaunchTask) {
            if (executor == null) {
                exitExecutor(1, "Received LaunchTask command but executor was null", null);
            } else {
                LaunchTask task = (LaunchTask) msg;
                TaskDescription taskDesc = TaskDescription.decode(task.taskData.buffer);
                LOGGER.info("Got assigned task {}", taskDesc.taskId);
                executor.launchTask(this, taskDesc);
            }
        } else if (msg instanceof KillTask) {
            if (executor == null) {
                exitExecutor(1, "Received KillTask command but executor was null", null);
            } else {
                KillTask task = (KillTask) msg;
                executor.killTask(task.taskId, task.interruptThread, task.reason);
            }
        } else if (msg instanceof StopExecutor) {
            stopping.set(true);
            LOGGER.info("Driver commanded a shutdown");
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
                    executor.stop();
                }
            }.start();
        }
    }

    @Override
    public void onDisconnected(RpcAddress remoteAddress) {
        if (stopping.get()) {
            LOGGER.info("Driver from {} disconnected during shutdown", remoteAddress.hostPort());
        } else if (driver != null && driver.address().equals(remoteAddress)){
            exitExecutor(1, format("Driver %s disassociated! Shutting down.", remoteAddress), null);
        } else {
            LOGGER.error("An unknown ({}) driver disconnected.", remoteAddress.hostPort());
        }
    }

    @Override
    public void statusUpdate(long taskId, TaskState state, ByteBuffer data) {
        StatusUpdate msg = new StatusUpdate(executorId, taskId, state, data);
        if (driver == null) {
            LOGGER.info("Drop {} because has not yet connected to driver", msg);
            return;
        }
        driver.send(msg);
    }

    private void exitExecutor(int code, String reason, Throwable throwable) {
        exitExecutor(code, reason, throwable, true);
    }

    private void exitExecutor(int code, String reason, Throwable throwable, boolean notifyDriver) {
        String message = format("Executor self-exiting due to : %s", reason);
        if (throwable != null) {
            LOGGER.error(message, throwable);
        } else {
            LOGGER.error(message);
        }
        if (notifyDriver && driver != null) {
            driver.ask(new RemoveExecutor(executorId, new ExecutorLossReason(reason)))
                  .exceptionally(t -> {
                      LOGGER.error("Unable to notify the driver due to {} ", t.getMessage(), t);
                      return t;
                  });
        }
        System.exit(code);
    }

    private static void run(String driverUrl,
                            String executorId,
                            String hostname,
                            int cores,
                            String appId,
                            String workerUrl,
                            URL[] userClassPath) {
        // step1: Driver地址、 配置信息
        SparkConf conf = new SparkConf();
        RpcEnv rpcEnv = RpcEnv.create("driverPropsFetcher",
                                      hostname,
                                      -1,
                                      conf,
                                      new SecurityManager(conf),
                                      true);

        RpcEndpointRef driverRef = rpcEnv.setupEndpointRefByURI(driverUrl);
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

            SparkEnv env = SparkEnv.createExecutorEnv(driverConf,
                                                      executorId,
                                                      hostname,
                                                      cores,
                                                      cfg.ioEncryptionKey,
                                                      false);

            env.rpcEnv.setRpcEndPointRef("Executor", new CoarseGrainedExecutorBackend(env.rpcEnv,
                                                                                      driverUrl,
                                                                                      executorId,
                                                                                      hostname,
                                                                                      cores,
                                                                                      userClassPath,
                                                                                      env));
            if (StringUtils.isNotEmpty(workerUrl)) {
                env.rpcEnv.setRpcEndPointRef("WorkerWatcher", new WorkerWatcher(env.rpcEnv, workerUrl));
            }

            env.rpcEnv.awaitTermination();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) {
        String driverUrl = null, executorId = null, hostname = null, appId = null, workerUrl = null;
        URL[] userClasspathUrl = null;
        int cores = 0;
        if (args != null && args.length > 0) {
            for (String arg : args) {
                String []p = arg.split(" ");
                switch (p[0]) {
                    case "--driver-url":
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
                    case "--worker-url":
                        workerUrl = p[1];
                        break;
                    case "--user-class-path":
                        String[] userClassPath = StringUtils.split(p[1], ";");
                        userClasspathUrl = new URL[userClassPath.length];
                        for (int i = 0; i < userClassPath.length; ++i) {
                            try {
                                userClasspathUrl[i] = new URL(userClassPath[i]);
                            } catch (Exception e) {
                                System.exit(-1);
                            }
                        }
                        break;
                }
            }

            run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClasspathUrl);
        }
    }
}
