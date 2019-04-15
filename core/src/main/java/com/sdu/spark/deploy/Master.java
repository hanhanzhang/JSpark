package com.sdu.spark.deploy;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.deploy.DeployMessage.*;
import com.sdu.spark.deploy.MasterMessage.*;
import com.sdu.spark.rpc.*;
import com.sdu.spark.utils.ThreadUtils;
import com.sun.corba.se.spi.orbutil.threadpool.Work;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.sdu.spark.network.utils.NettyUtils.getIpV4;
import static com.sdu.spark.utils.Utils.*;

/**
 * {@link Master}职责:
 *
 * 1: Worker节点管理
 *
 *    Worker节点启动时向Master发送Worker注册消息{@link RegisterWorker}且Worker节点启动定时线程向Master节点发送心跳消息{@link Heartbeat},
 *
 *    而Master节点在启动时启动定时线程向接收信箱投递Worker心跳超时消息{@link CheckForWorkerTimeOut}检测心跳超时的Worker
 *
 * 2: Application管理
 *
 *   {@link com.sdu.spark.SparkContext}初始化时启动SchedulerBackend({@link com.sdu.spark.scheduler.cluster.StandaloneSchedulerBackend}),
 *
 *   SchedulerBackend向Master发送注册Application消息{@link RegisterApplication}, Master调度ALIVE Worker并启动Executor进程
 *
 * 3: Application Driver管理
 *
 *    SparkSubmit提交Application时, 启动{@link com.sdu.spark.deploy.Client.ClientEndpoint}向Master注册Application
 *
 *    Driver{@link RequestSubmitDriver}(Driver注册先于Application注册), Master调度ALIVE Worker并启动Driver进程
 *
 * Note:
 *
 *   SparkSubmit提交Spark Job时, 程序启动顺序:
 *
 *   1: org.apache.spark.deploy.Client启动并向Master节点注册Spark Driver
 *
 *   2: Master接收Driver注册消息{@link RequestSubmitDriver}后, 调度ALIVE WORKER并向Worker节点发送启动Driver消息{@link LaunchDriver}
 *
 *   3: Worker接收Driver启动消息{@link LaunchDriver}后, 启动Driver进程(也就是用户编写的程序入口)
 *
 *   4: Driver进程启动中, 会初始化{@link com.sdu.spark.SparkContext}, SparkContext主要启动两类组件:
 *
 *     1': {@link com.sdu.spark.scheduler.SchedulerBackend}
 *
 *       SchedulerBackend通过{@link com.sdu.spark.deploy.client.StandaloneAppClient}向Master节点注册Spark Application{@link ApplicationDescription}
 *
 *       Master节点接收到Application注册消息{@link RegisterApplication}后, 调度可用Worker工作节点并向Worker节点发送启动Executor消息{@link LaunchExecutor}
 *
 *       Worker节点启动Executor进程后, 向{@link com.sdu.spark.deploy.client.StandaloneAppClient}发送Executor启动消息{@link ExecutorAdded}
 *
 *       Note:
 *
 *       StandaloneAppClient是Spark Application与Master通信客户端
 *
 *     2': {@link com.sdu.spark.scheduler.TaskScheduler}
 * */
public class Master extends ThreadSafeRpcEndpoint {

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
    private Map<RpcEndpointRef, ApplicationInfo> endpointToApp = Maps.newHashMap();
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
        LOGGER.info("master starting on address {}", address.toSparkURL());
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
        } else if (msg instanceof Heartbeat) {       // 工作节点心跳消息
            Heartbeat heartbeat = (Heartbeat) msg;
            String workerId = heartbeat.workerId;
            WorkerInfo workerInfo = idToWorker.get(workerId);
            if (workerInfo == null) {
                LOGGER.info("Got heartbeat from unregistered worker {}. Asking it to re-register.",
                            heartbeat.workerId, heartbeat.worker.address().hostPort());
                heartbeat.worker.send(new ReconnectWorker(self()));
            } else {
                LOGGER.info("Get heartbeat from worker {}, last heart timestamp {}",
                        heartbeat.workerId, workerInfo.lastHeartbeat);
                workerInfo.lastHeartbeat = System.currentTimeMillis();
            }
        } else if (msg instanceof RegisterWorker) {       // 注册工作节点
            RegisterWorker worker = (RegisterWorker) msg;
            LOGGER.info("Registering worker {}:{} with {} cores, {} RAM",
                        worker.host, worker.port, worker.cores, megabytesToString(worker.memory));
            if (idToWorker.containsKey(worker.workerId)) {
                worker.worker.send(new RegisterWorkerFailed("Duplicate worker ID"));
            } else {
                WorkerInfo workerInfo = new WorkerInfo(worker.workerId, worker.host,
                                                       worker.port, worker.cores,
                                                       worker.memory, worker.worker);
                if (registerWorker(workerInfo)) {
                    worker.worker.send(new RegisteredWorker(self()));
                    // 调度分配应用
                    schedule();
                } else {
                    RpcAddress workerAddress = worker.worker.address();
                    LOGGER.info("Worker registration failed. Attempted to re-register worker at same address: {}",
                                 workerAddress);
                    worker.worker.send(new RegisterWorkerFailed("Attempted to re-register worker at same address: "
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
    public void onDisconnected(RpcAddress remoteAddress) {
        LOGGER.info("{} got disassociated, removing it.", remoteAddress.hostPort());
        WorkerInfo workerInfo = addressToWorker.get(remoteAddress);
        if (workerInfo != null) {
            removeWorker(workerInfo, String.format("%s got disassociated", remoteAddress.hostPort()));
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
                removeWorker(oldWorker, "Worker replaced by a new worker with same address");
            } else {
                LOGGER.info("Attempted to re-register worker at same address: ", address);
                return true;
            }
        }
        idToWorker.put(worker.workerId, worker);
        addressToWorker.put(address, worker);
        workers.add(worker);
        return true;
    }

    private void removeWorker(WorkerInfo worker, String msg) {
        LOGGER.info("Removing worker {} on {}:{} ", worker.workerId, worker.host, worker.port);
        worker.state = WorkerState.DEAD;
        idToWorker.remove(worker.workerId);
        addressToWorker.remove(worker.endPointRef.address());

        // Worker节点部署的Executor
        if (MapUtils.isNotEmpty(worker.executors)) {
            for (ExecutorDesc exec : worker.executors.values()) {
                LOGGER.info("Telling app of lost executor: {}", exec.id);
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
                    LOGGER.info("Re-launching driver {}", driver.id);
                    relaunchDriver(driver);
                } else {
                    LOGGER.info("Not re-launching driver {} because it was not supervised", driver.id);
                    removeDriver(driver, DriverState.ERROR, null);
                }
            }
        }


        // 通知Application丢失Worker
        LOGGER.info("Telling app of lost worker: {}", worker.workerId);
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
            LOGGER.info("Got status update for unknown executor {}/{}", executor.appId, executor.executorId);
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
        LOGGER.info("Launching driver {} on worker {}", driver.id, worker.workerId);
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
                    .filter(worker -> worker.freeMemory() >= app.desc.memoryPerExecutorMB && worker.freeCores() >= coresPerExecutor)
                    .sorted(Comparator.comparingInt(WorkerInfo::freeCores))
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

    /**
     * @param coresToAssign 待分配CPU总核数
     * @param minCoresPerExecutor 每个Executor分配的最小CPU核数
     * @param memoryPerExecutor 每个Executor分配内存数
     * @param oneExecutorPerWorker 是否每个Worker分配一个Executor
     * @param assignedCores 每个Worker已分配CPU核数
     * @param assignedExecutors 每个Worker已分配Executor数
     * @param pos 第pos个Worker
     * @param worker Worker节点信息
     * */
    private boolean canLaunchExecutor(int coresToAssign,
                                      int minCoresPerExecutor,
                                      int memoryPerExecutor,
                                      boolean oneExecutorPerWorker,
                                      int []assignedCores,
                                      int []assignedExecutors,
                                      int pos,
                                      WorkerInfo worker) {
        // 判断当前Worker节点能否分配Executor, 需满足:
        // 1: 有充足的CPU核数
        // 2: 有充足的内存数
        boolean keepScheduling = coresToAssign >= minCoresPerExecutor;
        boolean enoughCores = worker.freeCores() - assignedCores[pos] >= minCoresPerExecutor;

        // 判断当前节点是否分配Executor的前提
        // 是否允许在Worker节点分配多个Executor, 若不允许, 则判断当前Worker是否分配过, 若分配过, 则不再分配
        if (!oneExecutorPerWorker || assignedExecutors[pos] == 0) {
            boolean enoughMemory = worker.freeMemory() - assignedExecutors[pos] * memoryPerExecutor >= memoryPerExecutor;
            // TODO: Executor Limit
            return keepScheduling && enoughCores && enoughMemory;
        }

        return keepScheduling && enoughCores;
    }

    /**
     * @return 返回每个Worker分配的CPU核数
     * */
    private int[] scheduleExecutorsOnWorkers(ApplicationInfo app, List<WorkerInfo> workers, boolean spreadOutApps) {
        /** 每个Worker分配CPU核数 */
        int []assignedCores = new int[workers.size()];
        /** 每个Worker分配的Executor数 */
        int []assignedExecutors = new int[workers.size()];
        /** 每个Executor分配CPU核数 */
        int coresPerExecutor = app.desc.coresPerExecutor;
        int minCoresPerExecutor = coresPerExecutor == -1 ? 1 : coresPerExecutor;
        boolean oneExecutorPerWorker = coresPerExecutor == -1;
        /** 每个Executor分配的内存数 */
        int memoryPerExecutor = app.desc.memoryPerExecutorMB;
        /** 保证空闲资源分配[若集群剩余资源 < app.coreLeft(), 则将资源全分配给App] */
        int coresToAssign = Math.min(app.coreLeft(), workers.stream().mapToInt(WorkerInfo::freeCores).sum());

        // 筛选资源充足的分配的Worker节点
        List<WorkerInfo> freeWorkers = Lists.newLinkedList();
        int pos = 0;
        for (WorkerInfo workerInfo : workers) {
            if (canLaunchExecutor(coresToAssign, minCoresPerExecutor, memoryPerExecutor, oneExecutorPerWorker, assignedCores, assignedExecutors, pos, workerInfo)) {
                freeWorkers.add(workerInfo);
            }
            pos += 1;
        }

        //
        while (!freeWorkers.isEmpty()) {
            pos = 0;
            for (WorkerInfo worker : freeWorkers) {
                boolean keepScheduling = true;

                // 默认Worker节点可分配多个Executor[若有Worker节点分配一个Executor, 则通过spreadOutApps控制]
                while (keepScheduling && canLaunchExecutor(coresToAssign, minCoresPerExecutor, memoryPerExecutor, oneExecutorPerWorker, assignedCores, assignedExecutors, pos, worker)) {
                    coresToAssign -= coresPerExecutor;
                    assignedCores[pos] += coresPerExecutor;

                    if (oneExecutorPerWorker) {
                        assignedExecutors[pos] = 1;
                    } else {
                        assignedExecutors[pos] += 1;
                    }

                    if (spreadOutApps) {
                        keepScheduling = false;
                    }
                }
                ++pos;
            }

            // 计算可用的资源
            pos = 0;
            freeWorkers = Lists.newLinkedList();
            for (WorkerInfo workerInfo : workers) {
                pos += 1;
                if (canLaunchExecutor(coresToAssign, minCoresPerExecutor, memoryPerExecutor, oneExecutorPerWorker, assignedCores, assignedExecutors, pos, workerInfo)) {
                    freeWorkers.add(workerInfo);
                }
            }
        }

        return assignedCores;
    }

    /**
     * @param assignedCores 当前Worker节点分配CPU核数
     * @param coresPerExecutor 每个Executor节点分配CPU核数
     * @param worker 部署的Worker节点
     * */
    private void allocateWorkerResourceToExecutors(ApplicationInfo app, int assignedCores, int coresPerExecutor, WorkerInfo worker) {
        if (coresPerExecutor == -1) {
            coresPerExecutor = 1;
        }
        int numExecutors = assignedCores / coresPerExecutor;
        for (int i = 1; i <= numExecutors; ++i) {
            ExecutorDesc desc = app.addExecutor(worker, coresPerExecutor);
            launchExecutor(worker, desc);
            app.state = ApplicationState.RUNNING;
        }

    }

    private void launchExecutor(WorkerInfo worker, ExecutorDesc exec) {
        LOGGER.info("Launch executor {} on worker {}:{}", worker.workerId, worker.host, exec.id);
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
                removeWorker(worker, String.format("Not receiving heartbeat for %d seconds", WORKER_TIMEOUT_MS / 1000));
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

        /**
         * RpcEnv创建流程:
         *
         *  1: 启动{@link com.sdu.spark.network.server.TransportServer}, 负责Rpc消息接收
         *
         *  2: RpcEnv注册{@link com.sdu.spark.rpc.netty.RpcEndpointVerifier}, 负责RpcEndPoint查询
         * */
        RpcEnv rpcEnv = RpcEnv.create(SYSTEM_NAME,
                                      args[0],
                                      convertStringToInt(args[1]),
                                      conf,
                                      securityManager
        );

        // 向RpcEnv注册Master节点
        Master master = new Master(conf, rpcEnv, rpcEnv.address());
        RpcEndpointRef masterRef = rpcEnv.setRpcEndPointRef(ENDPOINT_NAME, master);

        // 向Master的对应RpcEndPoint节点发送消息
        Future<BoundPortsResponse> future = masterRef.ask(new BoundPortsRequest());
        BoundPortsResponse response = getFutureResult(future);
        if (response != null) {
            LOGGER.info("spark master started and bind port {}", response.rpcEndpointPort);
        }

        rpcEnv.awaitTermination();
    }

}
