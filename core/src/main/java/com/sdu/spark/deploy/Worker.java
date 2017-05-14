package com.sdu.spark.deploy;

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

/**
 * 集群工作节点
 *
 * @author hanhan.zhang
 * */
public class Worker extends RpcEndPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    private RpcEnv rpcEnv;
    /**
     * Worker唯一标识
     * */
    private String workerId = generateWorkId();
    /**
     * Worker节点名称
     * */
    private String endPointName;
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
    private int memory;
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
    private RpcConfig config;
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

    private DateTimeFormatter createDateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public Worker(RpcConfig config, RpcEnv rpcEnv, String endPointName, int cores, int memory, RpcAddress masterRpcAddress, String workerDirPath) {
        this.config = config;
        this.rpcEnv = rpcEnv;
        this.endPointName = endPointName;
        this.cores = cores;
        this.memory = memory;
        this.masterRpcAddress = masterRpcAddress;
        this.workerDirPath = workerDirPath;
        scheduleMessageThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-schedule-message");
        registerExecutorService = ThreadUtils.newDaemonCachedThreadPool("worker-register-thread", 1, 60);
        // 防止数据丢包及网络延迟导致Master节点接收不到心跳
        heartbeatTimeInterval = this.config.getCheckWorkerTimeout() / 4;
    }

    public Worker(RpcEnv rpcEnv, RpcAddress masterRpcAddress) {
        this.rpcEnv = rpcEnv;
        this.masterRpcAddress = masterRpcAddress;
    }

    @Override
    public RpcEndPointRef self() {
        return rpcEnv.setRpcEndPointRef(endPointName, this);
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
    public void onStart() {
        LOGGER.info("Starting Spark worker {} with {} cores, {} RAM", rpcEnv.address().hostPort(),
                cores, memory);
        createDir();
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
    /**
     * 向Master节点注册Worker
     * */
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
                LOGGER.info("connect master : {}", address.hostPort());
                RpcEndPointRef masterPointRef = rpcEnv.setRpcEndPointRef(Master.MASTER, address);
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
            LOGGER.info("connect master {}", masterRpcAddress.hostPort());
            RpcEndPointRef masterPointRef = rpcEnv.setRpcEndPointRef(Master.MASTER, masterRpcAddress);
            sendRegisterMessageToMaster(masterPointRef);
        });
    }

    private void sendRegisterMessageToMaster(RpcEndPointRef masterRef) {
        masterRef.send(new RegisterWorker(workerId, host(), port(), cores, memory, self()));
    }

    /*******************************注册节点响应处理*********************************/

    /**
     * 注册工作节点响应处理
     * */
    private void handleRegisterResponse(RegisteredWorkerResponse msg) {
        if (msg instanceof RegisteredWorker) {
            RegisteredWorker registeredWorker = (RegisteredWorker) msg;
            LOGGER.info("Successfully registered with master {}", registeredWorker.getMaster().address().toSparkURL());
            register = true;
            master = registeredWorker.getMaster();
            cancelLastRegistrationRetry();
            /**
             * 向Master发送心跳消息
             * */
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
        return rpcEnv.address().getHost();
    }

    private int port() {
        return rpcEnv.address().getPort();
    }

    private String generateWorkId() {
        return "worker-" + LocalDateTime.now().format(createDateFormat) + "-" +
                host() + "-" + port();
    }
}
