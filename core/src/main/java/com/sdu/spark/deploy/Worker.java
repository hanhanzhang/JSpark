package com.sdu.spark.deploy;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.*;
import com.sdu.spark.deploy.WorkerLocalMessage.*;
import com.sdu.spark.deploy.DeployMessage.*;
import com.sdu.spark.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    public static final String ENDPOINT_NAME = "JSparkWorker";

    private RpcEnv rpcEnv;

    private DateTimeFormatter createDateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /**
     * Worker唯一标识
     * */
    private String workerId;
    /**
     * Master网络地址
     * */
    private RpcAddress masterRpcAddress;
    /**
     * 工作目录
     * */
    private String workerDirPath;
    /**
     * 工作节点CPU数量
     * */
    private int cores;
    /**
     * 工作节点内存
     * */
    private long memory;
    /**
     * 是否注解到Master
     * */
    private boolean register = false;
    /**
     * 发送心跳时间间隔
     * */
    private long heartbeatTimeInterval;
    /**
     * Rpc配置
     * */
    private JSparkConfig config;
    /**
     * Master节点引用
     * */
    private RpcEndPointRef master;
    /**
     * Worker定时消息线程[心跳发送]
     * */
    private ScheduledExecutorService scheduleMessageThread;
    /**
     * Worker注册Master线程
     * */
    private ExecutorService registerExecutorService;
    /**
     * 已向Master注册次数
     * */
    private int connectionAttemptCount = 0;

    private ScheduledFuture<?> registrationRetryTimer;
    private Future<?> registerMasterFuture;

    public Worker(JSparkConfig config, RpcEnv rpcEnv, int cores, long memory, RpcAddress masterRpcAddress, String workerDirPath) {
        this.config = config;
        this.rpcEnv = rpcEnv;
        this.cores = cores;
        this.memory = memory;
        this.masterRpcAddress = masterRpcAddress;
        this.workerDirPath = workerDirPath;
        scheduleMessageThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-schedule-message");
        registerExecutorService = ThreadUtils.newDaemonCachedThreadPool("worker-register-thread", 1, 60);
        // 防止数据丢包及网络延迟导致Master节点接收不到心跳
        heartbeatTimeInterval = this.config.getCheckWorkerTimeout() / 4;
        this.workerId = generateWorkId();
    }

    public Worker(RpcEnv rpcEnv, RpcAddress masterRpcAddress) {
        this.rpcEnv = rpcEnv;
        this.masterRpcAddress = masterRpcAddress;
    }

    @Override
    public RpcEndPointRef self() {
        return rpcEnv.endPointRef(this);
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
        }
    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {

    }

    @Override
    public void onStart() {
        LOGGER.info("JSpark Worker节点启动: hostPort = {}, JVM = {} RAM", rpcEnv.address().hostPort(),
                cores, memory);
//        createDir();
        startRegisterWithMaster();
    }

    @Override
    public void onEnd() {

    }

    @Override
    public void onStop() {

    }

    @Override
    public void onConnect(RpcAddress rpcAddress) {

    }

    @Override
    public void onDisconnect(RpcAddress rpcAddress) {

    }

    /**
     * 创建工作目录
     * */
    private void createDir() {
        File dir = new File(workerDirPath);
        dir.mkdirs();
        if (!dir.exists() || !dir.isDirectory()) {
            LOGGER.error("Failed to create work directory {}", workerDirPath);
            System.exit(-1);
        }
    }

    /*******************************向Master注册工作节点*****************************/
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
        } else if (connectionAttemptCount < config.getMaxRetryConnectTimes()) {
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
            if (connectionAttemptCount == config.getMaxRetryConnectTimes()) {
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
            LOGGER.info("JSpark Worker节点[{}]向JSpark Master[{}]注册", rpcEnv.address().hostPort(),
                    masterRpcAddress.hostPort());
            RpcEndPointRef masterPointRef = rpcEnv.setRpcEndPointRef(Master.ENDPOINT_NAME, masterRpcAddress);
            sendRegisterMessageToMaster(masterPointRef);
        });
    }

    private void sendRegisterMessageToMaster(RpcEndPointRef masterRef) {
        masterRef.send(new RegisterWorker(workerId, host(), port(), cores, memory, self()));
    }

    /*******************************注册节点响应处理*********************************/
    private void handleRegisterResponse(RegisteredWorkerResponse msg) {
        if (msg instanceof RegisteredWorker) {
            RegisteredWorker registeredWorker = (RegisteredWorker) msg;
            LOGGER.info("JSpark Worker节点成功注册到JSpark Master节点: {}", registeredWorker.master.address().toSparkURL());
            register = true;
            master = registeredWorker.master;
            cancelLastRegistrationRetry();
            // 向Master发送心跳消息
            scheduleMessageThread.scheduleWithFixedDelay(() -> {
                // 向本地投递心跳消息
                self().send(new SendHeartbeat());
            }, heartbeatTimeInterval, heartbeatTimeInterval, TimeUnit.SECONDS);
        } else if (msg instanceof RegisterWorkerFailed) {

        }
    }

    /**
     * 取消注册定时任务
     * */
    private void cancelLastRegistrationRetry() {
        if (registerMasterFuture != null) {
            registerMasterFuture.cancel(true);
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

        JSparkConfig sparkConfig = JSparkConfig.builder()
                                                .deliverThreads(1)
                                                .dispatcherThreads(1)
                                                .rpcConnectThreads(1)
                                                .maxRetryConnectTimes(2)
                                                .checkWorkerTimeout(10)
                                                .deadWorkerPersistenceTimes(2)
                                                .workerTimeout(20)
                                                .build();
        SecurityManager securityManager = new SecurityManager(sparkConfig);
        // 启动RpcEnv
        RpcEnv rpcEnv = RpcEnv.create(args[0], convertStringToInt(args[1]), sparkConfig, securityManager);

        Worker worker = new Worker(sparkConfig, rpcEnv, cpu, memory, masterAddress, "");

        // 向Worker's RpcEnv注册Worker节点
        // 向RpcEnv注册Worker节点时, 会向Index投递OnStart消息, 进而调用RpcEndPoint.onStart()方法
        rpcEnv.setRpcEndPointRef(ENDPOINT_NAME, worker);

        rpcEnv.awaitTermination();
    }
}
