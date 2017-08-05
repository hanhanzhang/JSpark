package com.sdu.spark.scheduler.cluster;

import com.sdu.spark.SparkContext;
import com.sdu.spark.deploy.ApplicationDescription;
import com.sdu.spark.deploy.Command;
import com.sdu.spark.deploy.client.StandaloneAppClient;
import com.sdu.spark.deploy.client.StandaloneAppClientListener;
import com.sdu.spark.laucher.LauncherBackend;
import com.sdu.spark.launcher.SparkAppHandle.State;
import com.sdu.spark.rpc.RpcEndpointAddress;
import com.sdu.spark.scheduler.TaskSchedulerImpl;
import com.sdu.spark.utils.DefaultFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.commons.lang3.StringUtils.split;

/**
 *
 * @author hanhan.zhang
 * */
public class StandaloneSchedulerBackend extends CoarseGrainedSchedulerBackend implements StandaloneAppClientListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandaloneSchedulerBackend.class);

    private SparkContext sc;
    private String master;

    private int maxCores;

    // Spark Master客户端[向Master注册App、向Master申请Executor、请求Master关闭Executor]
    private StandaloneAppClient client;
    // Spark Master客户端注册信号量
    private Semaphore registrationBarrier = new Semaphore(0);
    // Spark Master注册成功返回AppId
    private transient String appId;
    private AtomicBoolean stopping = new AtomicBoolean(false);
    private LauncherBackend launcherBackend = new LauncherBackend() {
        @Override
        public void onStopRequest() {
            stop(State.KILLED);
        }
    };

    public StandaloneSchedulerBackend(TaskSchedulerImpl scheduler, SparkContext sc, String master) {
        super(scheduler);
        this.maxCores = conf.getInt("spark.cores.max", Runtime.getRuntime().availableProcessors());
        this.sc = sc;
        this.master = master;
    }

    @Override
    public void start() {
        // step1: 启动Spark DriverEndPoint
        super.start();

        // step2: 构建CoarseGrainedExecutorBackend启动参数
        String driverUrl = RpcEndpointAddress.apply(
                sc.conf.get("spark.driver.host"),
                sc.conf.getInt("spark.driver.port", 6712),
                CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString();
        String[] args = new String[] {
                "--driver-url " + driverUrl,
                "--executor-id ",
                "--hostname ",
                "--cores ",
                "--app-id ",
                "--worker-url "
        };
        String[] extraJavaOpts = split(sc.conf.get("spark.executor.extraJavaOptions"), ',') ;
        String[] classPathEntries = split(sc.conf.get("spark.executor.extraClassPath"), File.pathSeparator);
        String[] libraryPathEntries = split(sc.conf.get("spark.executor.extraLibraryPath"), File.pathSeparator);

        Command command = new Command(
                "com.sdu.spark.executor.CoarseGrainedExecutorBackend",
                args,
                sc.executorEnvs,
                classPathEntries,
                libraryPathEntries,
                extraJavaOpts
        );
        int coresPerExecutor = conf.getInt("spark.executor.cores", 1);
        ApplicationDescription appDesc = new ApplicationDescription(
                sc.appName(),
                maxCores,
                coresPerExecutor,
                sc.executorMemory(),
                command
        );

        // step3: 启动Spark Master客户端StandaloneAppClient
        client = new StandaloneAppClient(sc.env.rpcEnv, master, appDesc, this, sc.conf);
        client.start();
        launcherBackend.setState(State.SUBMITTED);
        waitForRegistration();
        launcherBackend.setState(State.RUNNING);
    }

    @Override
    public void stop() {
        stop(State.FINISHED);
    }

    @Override
    public void connected(String appId) {
        LOGGER.info("Spark App[appId = {}]注册成功", appId);
        notifyContext();
        this.appId = appId;
        launcherBackend.setAppId(appId);
    }

    @Override
    public void disconnected() {
        notifyContext();
        if (!stopping.get()) {
            LOGGER.info("Spark Master断开连接, 等待重新注册...");
        }
    }

    @Override
    public void dead(String reason) {
        notifyContext();
        if (!stopping.get()) {
            launcherBackend.setState(State.KILLED);
            LOGGER.error("Spark App[appId = {}]被杀死, 原因: {}", appId, reason);
            try {
                scheduler.error(reason);
            } finally {
                // Ensure the application terminates, as we can no longer run jobs.
                sc.stopInNewThread();
            }
        }
    }

    @Override
    public void executorAdded(String fullId, String workerId, String hostPort, int cores, int memory) {
        LOGGER.info("Worker[host = {}]启动Executor[fullId = {}]: cores = {}, memory = {}",
                hostPort, fullId, cores, memory);
    }

    @Override
    public void executorRemove(String fullId, String message, int exitStatus, boolean workerLost) {
        LOGGER.info("Executor[fullId = {}]被移除, 原因: {}", fullId, message);
        removeExecutor(fullId.split("/")[1], message);
    }

    @Override
    public void workerRemoved(String workerId, String host, String message) {
        LOGGER.info("Worker[workerId = {}, host = {}]被移除, 原因: {}", workerId, host, message);
        removeWorker(workerId, host, message);
    }

    @Override
    public Future<Boolean> doRequestTotalExecutors(int requestedTotal) {
        if (client != null) {
            return client.requestTotalExecutors(requestedTotal);
        } else {
            return new DefaultFuture<>(false);
        }
    }

    @Override
    public Future<Boolean> doKillExecutors(List<String> executorIds) {
        if (client != null) {
            return client.killExecutors(executorIds);
        } else {
            return new DefaultFuture<>(false);
        }
    }

    @Override
    public boolean sufficientResourcesRegistered() {
        return totalCoreCount.get() >= maxCores * minRegisteredRatio;
    }

    private void stop(State finalState) {
        if (stopping.compareAndSet(false, true)) {
            try {
                super.stop();
                client.stop();
            } finally {
                launcherBackend.setState(finalState);
                launcherBackend.close();
            }
        }
    }

    private void waitForRegistration() {
        try {
            registrationBarrier.acquire();
        } catch (InterruptedException e) {
            System.exit(-1);
        }
    }

    private void notifyContext() {
        registrationBarrier.release();
    }
}
