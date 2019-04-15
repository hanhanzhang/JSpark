package com.sdu.spark.scheduler.cluster;

import com.sdu.spark.SparkContext;
import com.sdu.spark.deploy.ApplicationDescription;
import com.sdu.spark.deploy.Command;
import com.sdu.spark.deploy.client.StandaloneAppClient;
import com.sdu.spark.deploy.client.StandaloneAppClientListener;
import com.sdu.spark.executor.ExecutorExitCode.ExecutorExited;
import com.sdu.spark.executor.ExecutorExitCode.ExecutorLossReason;
import com.sdu.spark.executor.ExecutorExitCode.SlaveLost;
import com.sdu.spark.laucher.LauncherBackend;
import com.sdu.spark.launcher.SparkAppHandle.State;
import com.sdu.spark.rpc.RpcEndpointAddress;
import com.sdu.spark.scheduler.TaskSchedulerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.sdu.spark.utils.Utils.bytesToString;
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
        // step1: 启动Spark DriverEndpoint
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
                "",
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
        LOGGER.info("Connected to Spark cluster with app ID {}", appId);
        notifyContext();
        this.appId = appId;
        launcherBackend.setAppId(appId);
    }

    @Override
    public void disconnected() {
        notifyContext();
        if (!stopping.get()) {
            LOGGER.info("Disconnected from Spark cluster! Waiting for reconnection ...");
        }
    }

    @Override
    public void dead(String reason) {
        notifyContext();
        if (!stopping.get()) {
            launcherBackend.setState(State.KILLED);
            LOGGER.error("Application {} has been killed. Reason: {}", appId, reason);
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
        LOGGER.info("Granted executor ID {} on hostPort {} with {} cores, {} RAM",
                fullId, hostPort, cores, bytesToString(memory * 1024 * 1024));
    }

    @Override
    public void executorRemoved(String fullId, String message, int exitStatus, boolean workerLost) {
        ExecutorLossReason reason;
        if (exitStatus > 0) {
            reason = new ExecutorExited(exitStatus, true, message);
        } else {
            reason = new SlaveLost(message, workerLost);
        }
        removeExecutor(fullId.split("/")[1], reason);
    }

    @Override
    public void workerRemoved(String workerId, String host, String message) {
        LOGGER.info("Worker {} removed: {}", workerId, host, message);
        removeWorker(workerId, host, message);
    }

    @Override
    public CompletableFuture<Boolean> doRequestTotalExecutors(int requestedTotal) {
        if (client != null) {
            return client.requestTotalExecutors(requestedTotal);
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public CompletableFuture<Boolean> doKillExecutors(List<String> executorIds) {
        if (client != null) {
            return client.killExecutors(executorIds);
        } else {
            LOGGER.warn("Attempted to kill executors before driver fully initialized.");
            return CompletableFuture.completedFuture(false);
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
