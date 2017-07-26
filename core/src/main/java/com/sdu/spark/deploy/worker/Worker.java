package com.sdu.spark.deploy.worker;

import com.google.common.collect.Maps;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.deploy.ExecutorState;
import com.sdu.spark.deploy.Master;
import com.sdu.spark.rpc.*;
import com.sdu.spark.deploy.WorkerLocalMessage.*;
import com.sdu.spark.deploy.DeployMessage.*;
import com.sdu.spark.rpc.netty.NettyRpcEndPointRef;
import com.sdu.spark.rpc.netty.NettyRpcEnv;
import com.sdu.spark.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.*;

import static com.sdu.spark.network.utils.NettyUtils.getIpV4;
import static com.sdu.spark.utils.Utils.convertStringToInt;

/**
 * 集群工作节点
 *
 * @author hanhan.zhang
 * */
public class Worker extends RpcEndPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    public static final String ENDPOINT_NAME = "Worker";

    private SparkConf conf;

    private DateTimeFormatter createDateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");


    /********************************Master资源管理**********************************/
    // Master地址
    private RpcAddress masterRpcAddress;
    // 是否注解到Master
    private boolean register = false;
    // Master EndPoint节点引用
    private RpcEndPointRef master;
    // 心跳时间间隔
    private long HEARTBEAT_MILLIS = 60 * 1000L / 4L;
    // 心跳线程
    private ScheduledExecutorService scheduleMessageThread;
    // 注册线程
    private ExecutorService registerExecutorService;
    // 向Master注册次数
    private int connectionAttemptCount = 0;


    /*******************************Worker资源管理*******************************/
    // Worker唯一标识
    private String workerId;
    // 工作目录
    private File sparkHome;
    private File workerDir;
    // 分配CPU核数
    private int cores;
    // 分配JVm内存数
    private long memory;
    // 已使用CPU核数
    private int coresUsed = 0;
    // 已使用JVM内存数
    private long memoryUsed = 0;
    // Worker节点启动Driver进程集合[key = driverId, value = DriverRunner]
    private Map<String, DriverRunner> drivers = Maps.newHashMap();
    // Worker节点启动Executor进程集合[key = appId + "/" + execId, value = ExecutorRunner]
    private Map<String, ExecutorRunner> executors = Maps.newHashMap();


    private ScheduledFuture<?> registrationRetryTimer;
    private Future<?> registerMasterFuture;

    public Worker(SparkConf conf, RpcEnv rpcEnv, int cores, long memory, RpcAddress masterRpcAddress) {
        super(rpcEnv);
        this.conf = conf;
        this.cores = cores;
        this.memory = memory;
        this.masterRpcAddress = masterRpcAddress;
        this.sparkHome = new File(System.getenv().getOrDefault("SPARK_HOME", "."));
        scheduleMessageThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-schedule-message");
        registerExecutorService = ThreadUtils.newDaemonCachedThreadPool("worker-register-thread", 1, 60);
        // 防止数据丢包及网络延迟导致Master节点接收不到心跳
        HEARTBEAT_MILLIS = this.conf.getLong("spark.worker.timeout", 60L) * 1000L / 4L;
        this.workerId = generateWorkId();
    }

    public Worker(RpcEnv rpcEnv, RpcAddress masterRpcAddress) {
        super(rpcEnv);
        this.masterRpcAddress = masterRpcAddress;
    }

    @Override
    public void receive(Object msg) {
        if (msg instanceof RegisterWithMaster) {                    // 注册Worker节点
            registerWithMaster();
        } else if (msg instanceof RegisteredWorkerResponse) {       // 注册节点响应
            handleRegisterResponse((RegisteredWorkerResponse) msg);
        } else if (msg instanceof SendHeartbeat) {                  // 心跳消息处理
            if (master != null) {
                master.send(new WorkerHeartbeat(workerId, self()));
            }
        } else if (msg instanceof LaunchDriver) {                   // 启动Driver
            launchDriver((LaunchDriver) msg);
        } else if (msg instanceof LaunchExecutor) {
            launchExecutor((LaunchExecutor) msg);
        } else if (msg instanceof ExecutorStateChanged) {           // Executor状态变更
            handleExecutorStateChanged((ExecutorStateChanged) msg);
        } else if (msg instanceof KillExecutor) {
            // Spark Application运行结束, 工作节点关闭Executor进程
            killExecutor((KillExecutor) msg);
        }
    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {

    }

    @Override
    public void onStart() {
        LOGGER.info("Spark Worker节点启动: hostPort = {}, JVM = {} RAM", rpcEnv.address().hostPort(),
                cores, memory);
//        createWorkDir();
        startRegisterWithMaster();
    }

    /*******************************创建工作目录*************************************/
    private void createWorkDir() {
        workerDir = new File(sparkHome, "work");
        boolean result = workerDir.mkdirs();
        if (!result || !workerDir.isDirectory()) {
            LOGGER.error("Worker(workerId = {}, host = {})创建工作目录异常", workerId, host());
            System.exit(-1);
        }
    }

    /*******************************注册工作节点************************************/
    private void startRegisterWithMaster() {
        if (registrationRetryTimer == null) {
            scheduleMessageThread.scheduleWithFixedDelay(()-> {
                // 向Worker本地信息投递消息
                self().send(new RegisterWithMaster());
            }, 1, 10, TimeUnit.SECONDS);
        } else {
            LOGGER.info("Not spawning another attempt to register with the master, since there is an " +
                        "attempt scheduled already.");
        }
    }
    private void registerWithMaster() {
        if (register) {
            cancelLastRegistrationRetry();
        } else if (connectionAttemptCount < 16) {
            connectionAttemptCount++;
            if (master == null) {
                // 关闭已提交注册任务
                if (registerMasterFuture != null) {
                    registerMasterFuture.cancel(true);
                }
                registerMasterFuture = tryRegisterMaster();
            } else {
                if (registerMasterFuture != null) {
                    registerMasterFuture.cancel(true);
                }
                RpcAddress address = master.address();
                LOGGER.info("JSpark Master节点地址: {}", address.hostPort());
                RpcEndPointRef masterPointRef = rpcEnv.setRpcEndPointRef(Master.ENDPOINT_NAME, address);
                sendRegisterMessageToMaster(masterPointRef);
            }
            if (connectionAttemptCount == 16) {
                if (registrationRetryTimer != null) {
                    registrationRetryTimer = scheduleMessageThread.scheduleAtFixedRate(() -> {
                        // 向本地投递注册节点消息
                        self().send(new RegisterWithMaster());
                    }, 1, 10, TimeUnit.SECONDS);
                }
            }

        } else {
            LOGGER.error("master are unresponsive! Giving up.");
            System.exit(1);
        }
    }
    private Future<?> tryRegisterMaster() {
        return registerExecutorService.submit(() -> {
            RpcEndPointRef masterPointRef = rpcEnv.setRpcEndPointRef(Master.ENDPOINT_NAME, masterRpcAddress);
            sendRegisterMessageToMaster(masterPointRef);
        });
    }
    private void sendRegisterMessageToMaster(RpcEndPointRef masterRef) {
        LOGGER.info("Spark Worker节点[{}]尝试向Spark Master[{}]注册", rpcEnv.address().hostPort(),
                masterRef.address().hostPort());
        masterRef.send(new RegisterWorker(workerId, host(), port(), cores, memory, self()));
    }

    /*******************************注册节点响应处理*********************************/
    private void handleRegisterResponse(RegisteredWorkerResponse msg) {
        if (msg instanceof RegisteredWorker) {
            RegisteredWorker registeredWorker = (RegisteredWorker) msg;
            LOGGER.info("Spark Worker节点成功注册到Spark Master节点: {}", registeredWorker.master.address().toSparkURL());
            register = true;
            master = registeredWorker.master;
            cancelLastRegistrationRetry();
            // 向Master发送心跳消息
            scheduleMessageThread.scheduleWithFixedDelay(() -> {
                // 向本地投递心跳消息
                self().send(new SendHeartbeat());
            }, HEARTBEAT_MILLIS, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS);
        } else if (msg instanceof RegisterWorkerFailed) {

        }
    }

    /**********************************Worker启动Driver*************************************/
    private void launchDriver(LaunchDriver launchDriver) {
        LOGGER.info("工作节点启动Driver(driverId = {})进程", launchDriver.driverId);
        DriverRunner runner = new DriverRunner(conf, launchDriver.driverId, workerDir, sparkHome,
                                               launchDriver.desc, self(), null);
        drivers.put(launchDriver.driverId, runner);
        runner.start();
        coresUsed += launchDriver.desc.cores;
        memoryUsed += launchDriver.desc.mem;
    }

    /********************************Worker启动Executor进程*********************************/
    private void launchExecutor(LaunchExecutor launchExecutor) {
        File executorDir = new File(workerDir, launchExecutor.appId + "/" + launchExecutor.execId);
        LOGGER.info("工作节点启动Executor(execId = {}, appId = {})进程, 工作目录: {}",
                                launchExecutor.execId, launchExecutor.appId, executorDir.getAbsolutePath());
        if (!executorDir.mkdirs()) {
            throw new RuntimeException(String.format("Executor工作目录%s无法创建", executorDir.getAbsolutePath()));
        }

        String[] localAppDirs = new String[0];

        ExecutorRunner runner = new ExecutorRunner(launchExecutor.appId, launchExecutor.execId, launchExecutor.appDesc,
                                                   launchExecutor.cores, launchExecutor.memory, self(), sparkHome, executorDir, conf,
                                                   localAppDirs, ExecutorState.RUNNING);
        executors.put(launchExecutor.appId + "/" + launchExecutor.execId, runner);
        runner.start();
        coresUsed += launchExecutor.cores;
        memoryUsed += launchExecutor.memory;
        if (master != null) {
            LOGGER.info("Worker(workerId = {}, host = {})向Master发送Executor运行信息", workerId, host());
            master.send(new ExecutorStateChanged(launchExecutor.execId, launchExecutor.appId, runner.state,
                                                 "", 0));
        }
    }

    /********************************Worker关闭Executor进程***************************/
    private void killExecutor(KillExecutor executor) {
        String key = executor.appId + "/" + executor.execId;
        ExecutorRunner runner = executors.get(key);
        if (runner != null) {
            runner.kill();
        } else {
            LOGGER.info("关闭未知Executor(appId = {}, execId = {})", executor.appId, executor.execId);
        }
    }

    /***************************Worker Executor运行状态消息处理*************************/
    private void handleExecutorStateChanged(ExecutorStateChanged executorStateChanged) {

    }

    /**
     * 取消注册定时任务
     * */
    private void cancelLastRegistrationRetry() {
        if (registerMasterFuture != null) {
            registerMasterFuture.cancel(true)   ;
            registerMasterFuture = null;
        }
        if (registrationRetryTimer != null) {
            registrationRetryTimer.cancel(true);
            registrationRetryTimer = null;
        }
    }

    private String host() {
        return rpcEnv.address().host;
    }

    private int port() {
        return rpcEnv.address().port;
    }

    private String generateWorkId() {
        return "worker-" + LocalDateTime.now().format(createDateFormat) + "-" +
                host() + "-" + port();
    }

    public static void main(String[] args) {
        /**
         * 0: Worker绑定host地址
         * 1: Worker绑定port端口
         * 2: Worker注册的Master地址
         * 3: Worker启动分配的CPU数
         * 4: Worker启动分配的JVM数
         * */
        String ip = getIpV4();
        RpcAddress masterAddress = new RpcAddress(ip, 6712);
        int cpu = Runtime.getRuntime().availableProcessors() * 2;
        long memory = Runtime.getRuntime().maxMemory();
        args = new String[] {ip, "6713"};

        SparkConf conf = new SparkConf();
        conf.set("spark.rpc.deliver.message.threads", "32");
        conf.set("spark.rpc.netty.dispatcher.numThreads", "32");
        conf.set("spark.rpc.connect.threads", "32");
        conf.set("spark.worker.timeout", "1");
        conf.set("spark.dead.worker.persistence", "10");

        SecurityManager securityManager = new SecurityManager(conf);
        // 启动RpcEnv
        RpcEnv rpcEnv = RpcEnv.create(args[0], convertStringToInt(args[1]), conf, securityManager);

        Worker worker = new Worker(conf, rpcEnv, cpu, memory, masterAddress);

        // 向Worker's RpcEnv注册Worker节点
        // 向RpcEnv注册Worker节点时, 会向Index投递OnStart消息, 进而调用RpcEndPoint.onStart()方法
        rpcEnv.setRpcEndPointRef(ENDPOINT_NAME, worker);

        rpcEnv.awaitTermination();
    }
}
