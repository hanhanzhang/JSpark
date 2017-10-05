package com.sdu.spark.deploy;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.deploy.DeployMessage.*;
import com.sdu.spark.deploy.MasterMessage.*;
import com.sdu.spark.executor.Executor;
import com.sdu.spark.rpc.*;
import com.sdu.spark.utils.ThreadUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.sdu.spark.network.utils.NettyUtils.getIpV4;
import static com.sdu.spark.utils.Utils.convertStringToInt;
import static com.sdu.spark.utils.Utils.getFutureResult;

/**
 * 集群Master节点, 负责管理集群及Application信息
 *
 * ToDo:
 *
 *  1: 理解"Driver"与"Application"之间关系
 *
 *  2: 理解"Driver"的Server消息处理
 *
 * @author hanhan.zhang
 * */
public class Master extends RpcEndPoint {

    public static final Logger LOGGER = LoggerFactory.getLogger(Master.class);

    public static final String SYSTEM_NAME = "sparkMaster";
    public static final String ENDPOINT_NAME = "Master";

    /**
     * RpcMaster配置
     * */
    private SparkConf conf;
    /**
     * Master节点地址
     * */
    private RpcAddress address;
    /**
     * Master节点状态
     * */
    private RecoveryState state = RecoveryState.ALIVE;

    /***********************************Spark集群节点资源管理*************************************/
    // 集群工作节点
    private final Set<WorkerInfo> workers = new HashSet<>();
    // 工作节点地址信息[key = 工作节点地址, value = 工作节点信息]
    private Map<RpcAddress, WorkerInfo> addressToWorker = new HashMap<>();
    // 工作节点标识[key = 工作节点唯一标识, value = 工作节点信息]
    private Map<String, WorkerInfo> idToWorker = new HashMap<>();

    private ScheduledExecutorService messageThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread");
    // Spark Worker心跳超时检测时间间隔
    private long WORKER_TIMEOUT_MS = 60 * 1000L;
    // Spark Worker心跳超时允许次数
    private int REAPER_ITERATIONS = 15;

    /********************************JSpark集群应用(Application)管理*****************************/
    // 待分配Spark Application Driver
    private List<DriverInfo> waitingDrivers = Lists.newLinkedList();
    // 带分配Spark Application
    private List<ApplicationInfo> waitingApps = Lists.newLinkedList();
    // key = appId, value = ApplicationInfo
    private Map<String, ApplicationInfo> idToApp = Maps.newHashMap();
    // Spark集群Application集合
    private Set<ApplicationInfo> apps = Sets.newHashSet();
    // Spark与Application交互映射[key = Application引用, value = Application]
    private Map<RpcEndPointRef, ApplicationInfo> endpointToApp = Maps.newHashMap();
    private Map<RpcAddress, ApplicationInfo> addressToApp = Maps.newHashMap();
    // Spark集群已完成的Application
    private List<ApplicationInfo> completedApps = Lists.newArrayList();


    public Master(SparkConf conf, RpcEnv rpcEnv, RpcAddress address) {
        super(rpcEnv);
        this.conf = conf;
        this.address = address;
        WORKER_TIMEOUT_MS = conf.getLong("spark.worker.timeout", 60) * 1000L;
        REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15);
    }

    @Override
    public void onStart() {
        LOGGER.info("Spark Master节点启动：{}", address.toSparkURL());
        messageThread.scheduleWithFixedDelay(() -> self().send(new CheckForWorkerTimeOut()),
                                             0,
                                             WORKER_TIMEOUT_MS,
                                             TimeUnit.SECONDS);
    }

    @Override
    public void receive(Object msg) {
        if (msg instanceof ElectedLeader) {                 // Master选主处理

        } else if (msg instanceof CompleteRecovery) {       // Master恢复

        } else if (msg instanceof RevokedLeadership) {

        } else if (msg instanceof CheckForWorkerTimeOut) {
            timeoutDeadWorkers();
        } else if (msg instanceof WorkerHeartbeat) {       // 工作节点心跳消息
            WorkerHeartbeat heartbeat = (WorkerHeartbeat) msg;
            LOGGER.info("心跳: workerId = {}, hostPort = {}",
                    heartbeat.workerId, heartbeat.worker.address().hostPort());
            String workerId = heartbeat.workerId;
            WorkerInfo workerInfo = idToWorker.get(workerId);
            if (workerInfo == null) {
                LOGGER.info("尚未注册Worker心跳: workerId = {}, hostPort = {}",
                            heartbeat.workerId, heartbeat.worker.address().hostPort());
                heartbeat.worker.send(new ReconnectWorker(self()));
            } else {
                workerInfo.lastHeartbeat = System.currentTimeMillis();
            }
        } else if (msg instanceof RegisterWorker) {       // 注册工作节点
            RegisterWorker registerWorker = (RegisterWorker) msg;
            if (idToWorker.containsKey(registerWorker.workerId)) {
                registerWorker.worker.send(new RegisterWorkerFailed("Duplicate worker ID"));
            } else {
                WorkerInfo workerInfo = new WorkerInfo(registerWorker.workerId, registerWorker.host,
                                                       registerWorker.port, registerWorker.cores,
                                                       registerWorker.memory, registerWorker.worker);
                if (registerWorker(workerInfo)) {
                    registerWorker.worker.send(new RegisteredWorker(self()));
                    // 调度分配应用
                    schedule();
                } else {
                    RpcAddress workerAddress = registerWorker.worker.address();
                    LOGGER.info("Worker registration failed. Attempted to re-register worker at same " +
                            "address: {}", workerAddress);
                    registerWorker.worker.send(new RegisterWorkerFailed("Attempted to re-register worker at same address: "
                            + workerAddress));
                }
            }
        } else if (msg instanceof MasterChangeAcknowledged) {

        } else if (msg instanceof WorkerSchedulerStateResponse) {

        } else if (msg instanceof WorkerLatestState) {

        } else if (msg instanceof RegisterApplication) {    // 注册应用

        } else if (msg instanceof UnregisterApplication) {  // 移除应用

        } else if (msg instanceof ExecutorStateChanged) {   // Executor变化
            handleExecutorStateChanged((ExecutorStateChanged) msg);
        } else if (msg instanceof DriverStateChanged) {

        }
    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof RequestSubmitDriver) {           // 请求注册Driver

        } else if (msg instanceof RequestKillDriver) {      // 杀死Driver

        } else if (msg instanceof RequestDriverStatus) {

        } else if (msg instanceof RequestMasterState) {

        } else if (msg instanceof BoundPortsRequest) {
            context.reply(new BoundPortsResponse(address.port, address.port));
        } else if (msg instanceof RequestExecutors) {

        } else if (msg instanceof KillExecutors) {

        }  else {
            throw new IllegalArgumentException("Unknown message");
        }
    }

    @Override
    public void onDisconnect(RpcAddress remoteAddress) {
        LOGGER.info("删除断开连接的工作节点worker(address = {})", remoteAddress.hostPort());
        WorkerInfo workerInfo = addressToWorker.get(remoteAddress);
        if (workerInfo != null) {
            removeWorker(workerInfo, String.format("worker(%s)断开连接", remoteAddress.hostPort()));
        }

        ApplicationInfo app = addressToApp.get(remoteAddress);
        if (app != null) {
            finishApplication(app);
        }

        if (state == RecoveryState.RECOVERING && canCompleteRecovery()) { 
            completeRecovery();
        }
    }

    private boolean registerWorker(WorkerInfo worker) {
        // 删除已挂掉的worker及当前注册work地址相同
        Iterator<WorkerInfo> it = workers.iterator();
        while (it.hasNext()) {
            WorkerInfo w = it.next();
            if (w.host.equals(worker.host) && w.port == worker.port &&
                    w.state == WorkerState.DEAD) {
                it.remove();
            }
        }
        RpcAddress address = worker.endPointRef.address();
        if (addressToWorker.containsKey(address)) {
            WorkerInfo oldWorker = addressToWorker.get(address);
            if (oldWorker.state == WorkerState.UNKNOWN) {
                removeWorker(oldWorker, "被相同地址的工作节点替换");
            } else {
                LOGGER.info("重复注册地址相同的Worker({})工作节点", address);
                return true;
            }
        }
        idToWorker.put(worker.workerId, worker);
        addressToWorker.put(address, worker);
        workers.add(worker);
        return true;
    }

    private void removeWorker(WorkerInfo worker, String msg) {
        LOGGER.info("移除主机(host = {}, port = {})上worker(id = {})节点", worker.host, worker.port, worker.workerId);
        worker.state = WorkerState.DEAD;
        idToWorker.remove(worker.workerId);
        addressToWorker.remove(worker.endPointRef.address());

        // Worker节点部署的Executor
        if (MapUtils.isNotEmpty(worker.executors)) {
            for (ExecutorDesc exec : worker.executors.values()) {
                LOGGER.info("告知Application的Executor丢失消息");
                exec.application.driver.send(new ExecutorUpdated(
                        exec.id,
                        ExecutorState.LOST,
                        "worker lost",
                        -1,
                        true
                ));
                exec.state = ExecutorState.LOST;
                exec.application.removeExecutor(exec);
            }
        }


        // Worker节点部署的Driver
        if (MapUtils.isNotEmpty(worker.drivers)) {
            for (DriverInfo driver : worker.drivers.values()) {
                if (driver.desc.supervise) {
                    LOGGER.info("重新启动Driver(id = {})", driver.id);
                    relaunchDriver(driver);
                } else {
                    LOGGER.info("由于Driver(id = {})不支持重启, 放弃", driver.id);
                    removeDriver(driver, DriverState.ERROR, null);
                }
            }
        }


        // 通知Application丢失Worker
        LOGGER.info("通知Application丢失Worker节点：{}", worker.workerId);
        apps.stream().filter(app -> !completedApps.contains(app))
                     .forEach(app -> app.driver.send(new WorkerRemoved(
                             worker.workerId,
                             worker.host,
                             msg
                     )));
        // TODO: PersistenceEngine
    }

    private void finishApplication(ApplicationInfo app) {
        removeApplication(app, ApplicationState.FINISHED);
    }

    private boolean canCompleteRecovery() {
        int workerState = 0;
        for (WorkerInfo worker : workers) {
            if (worker.state == WorkerState.UNKNOWN) {
                workerState++;
            }
        }
        int appState = 0;
        for (ApplicationInfo app : apps) {
            if (app.state == ApplicationState.UNKNOWN) {
                ++appState;
            }
        }
        return workerState == 0 && appState == 0;
    }

    private void completeRecovery() {
        // TODO:
    }

    private void relaunchDriver(DriverInfo driver) {
        // TODO:
    }

    private void removeDriver(DriverInfo driver, DriverState state, Exception e) {

    }
    
    /********************************Spark Executor管理*******************************/
    private void handleExecutorStateChanged(ExecutorStateChanged executor) {
        ExecutorDesc desc = idToApp.get(executor.appId).executors.get(executor.executorId);
        if (desc == null) {
            LOGGER.info("收到未知应用(appId = {})的Executor(execId = {})的状态变更", executor.appId, executor.executorId);
            return;
        }
        ApplicationInfo appInfo = idToApp.get(executor.appId);
        desc.state = executor.state;

        desc.application.driver.send(new ExecutorUpdated(desc.id, desc.state, executor.message, executor.exitStatus, false));

        if (ExecutorState.isFinished(desc.state)) {     // Executor进程结束
            if (appInfo.isFinished()) {
                appInfo.removeExecutor(desc);
            }

            desc.worker.removeExecutor(desc);
            // 删除应用
            if (executor.exitStatus != 0) {
                Collection<ExecutorDesc> executors = appInfo.executors.values();
                if (executors.stream().filter(exec -> exec.state == ExecutorState.RUNNING).count() == 0) {
                    removeApplication(appInfo, ApplicationState.FINISHED);
                }
            }
        }
        // 调度Spark应用
        schedule();
    }

    private void removeApplication(ApplicationInfo app, ApplicationState state) {
        if (apps.contains(app)) {
            apps.remove(app);
            idToApp.remove(app.id);
            endpointToApp.remove(app.driver);
            addressToApp.remove(app.driver.address());
            completedApps.add(app);

            // 关闭Application的Executor
            app.executors.values().forEach(this::killExecutor);
            app.markFinished(state);
            if (app.state != ApplicationState.FINISHED) {
                app.driver.send(new ApplicationRemoved(app.state.name()));
            }

//            persistenceEngine.removeApplication(app)
            schedule();

            // 通知Worker节点, 更新Application状态
            workers.forEach(worker -> worker.endPointRef.send(new ApplicationFinished(app.id)));
        }
    }

    /********************************Spark Executor进程关闭*****************************/
    private void killExecutor(ExecutorDesc exec) {
        exec.worker.removeExecutor(exec);
        // 通知Worker节点关闭Executor
        exec.worker.endPointRef.send(new KillExecutor(exec.application.id, exec.id));
        exec.state = ExecutorState.KILLED;
    }

    /********************************Spark应用调度资源**********************************/
    private void schedule() {
        if (state != RecoveryState.ALIVE) {
            return;
        }

        // 过滤已挂掉Worker节点
        List<WorkerInfo> aliveWorkers = workers.stream().filter(WorkerInfo::isAlive).collect(Collectors.toList());
        Collections.shuffle(aliveWorkers);
        int numWorkersAlive = aliveWorkers.size();
        int curPos = 0;

        /**
         *
         * 启动Driver(即: 用户编写Spark程序入口)
         *
         * 1: 随机选择Spark Driver运行资源充足的Worker节点(即: 满足Driver要求的CPU数、JVM内存数)
         *
         * 2: 向Worker节点发送消息{@link LaunchDriver}
         * */
        Iterator<DriverInfo> it = waitingDrivers.iterator();
        while (it.hasNext()) {
            DriverInfo driverInfo = it.next();
            boolean launched = false;
            int numWorkersVisited = 0;

            while (numWorkersVisited < numWorkersAlive && !launched) {
                WorkerInfo workerInfo = aliveWorkers.get(curPos);
                numWorkersVisited += 1;
                if (workerInfo.freeMemory() >= driverInfo.desc.mem &&
                        workerInfo.freeCores() >= driverInfo.desc.cores) {
                    launched = true;
                    it.remove();
                    //
                    launchDriver(workerInfo, driverInfo);
                }

                curPos = (curPos + 1) % numWorkersAlive;
            }
        }

        /**
         * Spark任务分配Executor(Executor为JVM进程)
         * */
        startExecutorsOnWorkers();
    }

    private void launchDriver(WorkerInfo worker, DriverInfo driver) {
        LOGGER.info("工作节点(workerId = {}, host = {})启动Spark应用(driverId = {})",
                worker.workerId, worker.host, driver.id);
        worker.addDriver(driver);
        driver.worker = worker;
        worker.endPointRef.send(new LaunchDriver(driver.id, driver.desc));
        driver.state = DriverState.RUNNING;
    }

    private void startExecutorsOnWorkers() {
        waitingApps.stream().filter(app -> app.coreLeft() > 0).forEach(app -> {
            int coresPerExecutor = app.desc.coresPerExecutor;
            /**
             * Spark任务启动Executor
             *
             * 1: scheduleExecutorsOnWorkers
             *
             *  计算每个Worker分配的CPU数(Note: 返回数组与Worker列表对应)
             *
             *  Note:
             *
             *      scheduleExecutorsOnWorkers第三个参数标识是否一个Worker节点启动一个Executor
             *
             * 2: Worker节点启动Executor(Note: Worker节点分配的CPU核数不等于零)
             *
             * 3: allocateWorkerResourceToExecutors
             *
             *  1': 根据Worker分配的CPU核数及每个Executor, 计算Worker节点需启动Executor数
             *
             *  2': 向Worker节点发送消息{@link LaunchExecutor}
             *
             *  3': 向Driver节点发送消息{@link ExecutorAdded}
             *
             * */
            List<WorkerInfo> usableWorkers = workers.stream().filter(worker -> worker.state == WorkerState.ALIVE)
                    .filter(worker -> worker.freeMemory() >= app.desc.memoryPerExecutorMB &&
                                      worker.freeCores() >= coresPerExecutor)
                    .sorted((worker1, worker2) -> worker1.freeCores() - worker2.freeCores())
                    .collect(Collectors.toList());

            // 计算每个Worker节点分配CPU数(assignedCores[n]表示usableWorkers.get(n)节点可分配CPU数)
            int[] assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, true);

            // 在Worker节点启动Executor
            for (int i = 0; i < assignedCores.length; ++i) {
                int assignCores = assignedCores[i];
                if (assignCores != 0) {
                    allocateWorkerResourceToExecutors(app, assignCores, coresPerExecutor, usableWorkers.get(i));
                }
            }
        });
    }

    private List<WorkerInfo> canLaunchWorker(int coresToAssign, int []assignedCores, int []assignedExecutors,
                                             int coresPerExecutor, int memoryPerExecutor, List<WorkerInfo> workers) {
        int pos = 0;
        List<WorkerInfo> freeWorkers = Lists.newLinkedList();
        for (WorkerInfo worker : workers) {
            if (canLaunchExecutor(coresToAssign, assignedCores, assignedExecutors, worker, coresPerExecutor, memoryPerExecutor, pos)) {
                freeWorkers.add(worker);
            }
            ++pos;
        }
        return freeWorkers;
    }

    private boolean canLaunchExecutor(int coresToAssign, int []assignedCores, int []assignedExecutors,
                                      WorkerInfo worker, int minCoresPerExecutor, int memoryPerExecutor,
                                      int pos) {
        boolean keepScheduling = coresToAssign >= minCoresPerExecutor;
        boolean enoughCores = worker.freeCores() - assignedCores[pos] >= minCoresPerExecutor;

        if (assignedExecutors[pos] == 0) {
            boolean enoughMemory = worker.freeMemory() - assignedExecutors[pos] * memoryPerExecutor >= memoryPerExecutor;
            return keepScheduling && enoughCores && enoughMemory;
        } else {
            return keepScheduling && enoughCores;
        }
    }

    private int[] scheduleExecutorsOnWorkers(ApplicationInfo app, List<WorkerInfo> workers, boolean spreadOutApps) {
        // 每个Worker分配CPU数
        int []assignedCores = new int[workers.size()];
        // 每个Worker
        int []assignedExecutors = new int[workers.size()];

        int coresPerExecutor = app.desc.coresPerExecutor;
        int memoryPerExecutor = app.desc.memoryPerExecutorMB;
        // 保证空闲资源分配[若集群剩余资源 < app.coreLeft(), 则将资源全分配给App]
        int coresToAssign = Math.min(app.coreLeft(), (int) workers.stream().map(WorkerInfo::freeCores).count());

        // 过滤可分配Executor的Worker节点
        List<WorkerInfo> freeWorkers = canLaunchWorker(coresToAssign, assignedCores, assignedExecutors, coresPerExecutor, memoryPerExecutor, workers);

        while (!freeWorkers.isEmpty()) {
            int i = 0;
            for (WorkerInfo worker : freeWorkers) {
                boolean keepScheduling = true;

                // 默认Worker节点可分配多个Executor[若有Worker节点分配一个Executor, 则通过spreadOutApps控制]
                while (keepScheduling && canLaunchExecutor(coresToAssign, assignedCores, assignedExecutors, worker, coresPerExecutor, memoryPerExecutor, i)) {
                    coresToAssign -= coresPerExecutor;
                    assignedCores[i] += coresPerExecutor;
                    assignedExecutors[i] += 1;
                    if (spreadOutApps) {
                        keepScheduling = false;
                    }
                }

                freeWorkers = canLaunchWorker(coresToAssign, assignedCores, assignedExecutors, coresPerExecutor, memoryPerExecutor, workers);
                ++i;
            }
        }

        return assignedCores;
    }

    private void allocateWorkerResourceToExecutors(ApplicationInfo app, int assignedCores,
                                                   int coresPerExecutor, WorkerInfo worker) {
        int numExecutors = assignedCores / coresPerExecutor;
        for (int i = 1; i <= numExecutors; ++i) {
            ExecutorDesc desc = app.addExecutor(worker, coresPerExecutor);
            launchExecutor(worker, desc);
            app.state = ApplicationState.RUNNING;
        }

    }

    private void launchExecutor(WorkerInfo worker, ExecutorDesc exec) {
        LOGGER.info("在工作节点(workerId = {}, host = {})启动执行器(executorId = {})", worker.workerId, worker.host, exec.id);
        worker.addExecutor(exec);
        worker.endPointRef.send(new LaunchExecutor(exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory));
        exec.application.driver.send(new ExecutorAdded(exec.id, worker.workerId, worker.host, exec.cores, exec.memory));
    }

    /**
     * 摘掉心跳超时工作节点
     * */
    private void timeoutDeadWorkers() {
        Iterator<WorkerInfo> it = workers.iterator();
        while (it.hasNext()) {
            WorkerInfo worker = it.next();
            if (worker.lastHeartbeat < System.currentTimeMillis() - WORKER_TIMEOUT_MS &&
                    worker.state != WorkerState.DEAD) {
                LOGGER.info("Removing worker {} because we got no heartbeat in {} seconds", worker.workerId,
                        WORKER_TIMEOUT_MS);
                removeWorker(worker, String.format("超过%ds未收心跳", WORKER_TIMEOUT_MS / 1000));
                it.remove();
            } else {
                if (worker.lastHeartbeat < System.currentTimeMillis() - (REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS) {
                    it.remove();
                }
            }
        }
    }

    public static void main(String[] args) {
        String ip = getIpV4();

        args = new String[]{ip, "6712"};

        SparkConf conf = new SparkConf();
        conf.set("spark.rpc.deliver.message.threads", "32");
        conf.set("spark.rpc.netty.dispatcher.numThreads", "32");
        conf.set("spark.rpc.connect.threads", "32");
        conf.set("spark.worker.timeout", "10");
        conf.set("spark.dead.worker.persistence", "10");

        SecurityManager securityManager = new SecurityManager(conf);

        // 启动RpcEnv
        RpcEnv rpcEnv = RpcEnv.create(
                SYSTEM_NAME,
                args[0],
                convertStringToInt(args[1]),
                conf,
                securityManager
        );

        // 向RpcEnv注册Master节点
        Master master = new Master(conf, rpcEnv, rpcEnv.address());
        RpcEndPointRef masterRef = rpcEnv.setRpcEndPointRef(ENDPOINT_NAME, master);

        // 向Master的对应RpcEndPoint节点发送消息
        Future<?> future = masterRef.ask(new BoundPortsRequest());
        BoundPortsResponse response = getFutureResult(future);
        if (response != null) {
            LOGGER.info("Master绑定端口: {}", response.rpcEndpointPort);
        }

        rpcEnv.awaitTermination();
    }

}
