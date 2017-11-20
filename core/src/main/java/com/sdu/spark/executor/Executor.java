package com.sdu.spark.executor;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sdu.spark.*;
import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.rpc.RpcEndpointRef;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.*;
import com.sdu.spark.scheduler.TaskEndReason.TaskFailedReason;
import com.sdu.spark.scheduler.TaskEndReason.TaskKilled;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.shuffle.FetchFailedException;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockId.TaskResultBlockId;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.sdu.spark.executor.ExecutorExitCode.HEARTBEAT_FAILURE;
import static com.sdu.spark.scheduler.TaskState.FINISHED;
import static com.sdu.spark.scheduler.TaskState.RUNNING;
import static com.sdu.spark.utils.RpcUtils.makeDriverRef;
import static com.sdu.spark.utils.ThreadUtils.newDaemonCachedThreadPool;
import static com.sdu.spark.utils.ThreadUtils.newDaemonSingleThreadScheduledExecutor;
import static com.sdu.spark.utils.Utils.*;
import static java.lang.Thread.currentThread;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * {@link Executor}职责:
 *
 * 1: Executor向Driver上报心跳(HeartBeatReceiver), 若是心跳响应标明BlockManager尚未向Driver BlockManagerMaster注册,
 *
 *    则BlockManagerMaster向Driver BlockManagerMaster注册BlockManager信息, 其调用链:
 *
 *    Executor.startDriverHeartbeat(Heartbeat)
 *      |
 *      |   未注册BlockManager
 *      +----------------------> BlockManagerMaster.reregister(RegisterBlockManager)
 *
 * 2: Executor Shuffle
 *
 * 3: Executor Kill运行中Task
 *
 * @author hanhan.zhang
 * */
public class Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Executor.class);

    private ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

    /****************************Spark运行环境*******************************/
    private boolean isLocal;
    private String executorId;
    private SparkEnv env;
    private SparkConf conf;

    private URL[] userClassPath;
    // Application dependencies (added through SparkContext) that we've fetched so far on this node.
    // Each map holds the master's timestamp for the version of that file or JAR we got.
    private Map<String, Long> currentFiles = Maps.newHashMap();
    private Map<String, Long> currentJars = Maps.newHashMap();

    /****************************Spark Task*********************************/
    // 任务线程
    private ThreadPoolExecutor threadPool;
    private boolean taskReaperEnabled;
    // 任务取消线程
    private ThreadPoolExecutor taskReaperPool = newDaemonCachedThreadPool("Task reaper");
    private final Map<Long, TaskReaper> taskReaperForTask = Maps.newHashMap();
    // 运行中任务集合[key = taskId, value = TaskRunner]
    private Map<Long, TaskRunner> runningTasks = Maps.newConcurrentMap();
    // 向Driver发现心跳
    private ScheduledExecutorService heartbeater = newDaemonSingleThreadScheduledExecutor("driver-heartbeater");
    private RpcEndpointRef heartbeatReceiverRef;
    private int heartbeatFailures = 0;
    private int HEARTBEAT_MAX_FAILURES;

    /****************************Spark Task Result**************************/
    private int maxResultSize;
    // Max size of direct result. If task result is bigger than this, we use the block manager
    // to send the result back.
    private int maxDirectResultSize;
    // Whether to load classes in user jars before those in Spark jars
    private boolean userClassPathFirst;

    // Create our ClassLoader
    // do this after SparkEnv creation so can access the SecurityManager
    private ClassLoader urlClassLoader;
    private ClassLoader replClassLoader;

    /*************************Spark Thread Exception Handler****************/
    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    private static ThreadLocal<Properties> taskDeserializationProps = new ThreadLocal<>();

    public Executor(String executorId, String executorHostname, SparkEnv env, URL[] userClassPath) {
        this(executorId, executorHostname, env, false, userClassPath, new SparkUncaughtExceptionHandler());
    }

    public Executor(String executorId,
                    String executorHostname,
                    SparkEnv env,
                    boolean isLocal,
                    URL[] userClassPath,
                    Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        LOGGER.info("Starting executor Id {} on host {}", executorId, executorHostname);
        Utils.checkHost(executorHostname);
        // must not have port specified
        assert Utils.parseHostPort(executorHostname)._2() == 0;
        // Make sure the local hostname we report matches the cluster scheduler's name for this host
        Utils.setCustomHostname(executorHostname);

        if (!isLocal) {
            // Setup an uncaught exception handler for non-local mode.
            // Make any thread terminations due to uncaught exceptions kill the entire
            // executor process to avoid surprising stalls.
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
                                                                .setNameFormat("Executor task launch worker-%d")
                                                                .setThreadFactory(r -> {
                                                                    // Use UninterruptibleThread to run tasks so that we can allow running codes without being
                                                                    // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
                                                                    // will hang forever if some methods are interrupted.
                                                                    return new UninterruptibleThread(r, "unused");
                                                                })
                                                                .build();
        this.threadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool(threadFactory);

        this.executorId = executorId;
        this.env = env;
        this.conf = env.conf;
        this.isLocal = isLocal;
        this.userClassPath = userClassPath;
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        this.taskReaperEnabled = conf.getBoolean("spark.task.reaper.enabled", false);
        this.userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false);
        HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60);
        maxResultSize = conf.getInt("spark.driver.maxResultSize", 1024);
        maxDirectResultSize = conf.getInt("spark.task.maxDirectResultSize", 10240);
        heartbeatReceiverRef = makeDriverRef(HeartBeatReceiver.ENDPOINT_NAME, conf, this.env.rpcEnv);

        urlClassLoader = createClassLoader();
        replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader);

        if (!isLocal) {
            this.env.blockManager.initialize(this.conf.getAppId());
        }

        startDriverHeartbeat();
    }

    private void startDriverHeartbeat() {
        long intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s");
        long initialDelay = (long) (intervalMs + Math.random() * intervalMs);
        heartbeater.scheduleAtFixedRate(this::reportHeartBeat,
                                        initialDelay,
                                        intervalMs,
                                        TimeUnit.MILLISECONDS);
    }

    private void reportHeartBeat() {
        // TODO: Task Accumulator
        Heartbeat message = new Heartbeat(executorId, env.blockManager.blockManagerId);
        try {
            long timeout = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s");
            HeartbeatResponse response = (HeartbeatResponse) heartbeatReceiverRef.askSync(message, timeout);
            if (response.registerBlockManager) {
                LOGGER.info("Told to re-register on heartbeat");
                env.blockManager.reregister();
            }
            heartbeatFailures = 0;
        } catch (Exception e) {
            heartbeatFailures += 1;
            if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
                LOGGER.error("Exit as unable to send heartbeats to driver more than {} times", heartbeatFailures);
                System.exit(HEARTBEAT_FAILURE);
            }
        }

    }

    public void launchTask(ExecutorBackend context, TaskDescription taskDescription) {
        TaskRunner tr = new TaskRunner(context, taskDescription);
        runningTasks.put(taskDescription.taskId, tr);
        threadPool.execute(tr);
    }


    /******************************Executor杀死Task***************************/
    public void killTask(long taskId, boolean interruptThread, String reason) {
        TaskRunner taskRunner = runningTasks.get(taskId);
        if (taskRunner != null) {
            if (taskReaperEnabled) {
                TaskReaper maybeNewTaskReaper = null;
                synchronized (taskReaperForTask) {
                    TaskReaper reaper = taskReaperForTask.get(taskId);
                    boolean shouldCreateReaper = false;
                    if (reaper == null) {
                        shouldCreateReaper = true;
                    } else {
                        shouldCreateReaper = interruptThread && !reaper.interruptThread;
                    }
                    if (shouldCreateReaper) {
                        TaskReaper taskReaper = new TaskReaper(taskRunner, interruptThread, reason);
                        taskReaperForTask.put(taskId, taskReaper);
                        maybeNewTaskReaper = taskReaper;
                    }
                }
                if (maybeNewTaskReaper != null) {
                    taskReaperPool.execute(maybeNewTaskReaper);
                }
            } else {
                taskRunner.kill(interruptThread, reason);
            }
        }
    }

    public void killAllTasks(boolean interruptThread, String reason) {
        runningTasks.keySet().forEach(taskId -> killTask(taskId, interruptThread, reason));
    }

    /**
     * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
     * created by the interpreter to the search path
     * */
    private MutableURLClassLoader createClassLoader() {
        // Bootstrap the list of jars with the user class path.
        long now = System.currentTimeMillis();
        for (URL url : userClassPath) {
            String[] paths = url.getPath().split("/");
            currentJars.put(paths[paths.length - 1], now);
        }

        // TODO: why?
        ClassLoader currentLoader = getContextOrSparkClassLoader();

        // For each of the jars in the jarSet, add them to the class loader.
        // We assume each of the files has already been fetched.
        List<URL> urls = currentJars.keySet().stream()
                .map(uri -> {
                    try {
                        String[] paths = uri.split("/");
                        String path = paths[paths.length - 1];
                        return new File(path).toURI().toURL();
                    } catch (Exception e) {
                        // ignore
                        throw new SparkException(e);
                    }
                }).collect(Collectors.toList());
        URL[] allUrls = ArrayUtils.addAll(userClassPath, urls.toArray(new URL[urls.size()]));
        if (userClassPathFirst) {
            return new ChildFirstURLClassLoader(allUrls, currentLoader);
        } else {
            return new MutableURLClassLoader(allUrls, currentLoader);
        }
    }

    @SuppressWarnings("unchecked")
    private ClassLoader addReplClassLoaderIfNeeded(ClassLoader parent) {
        String classUri = conf.get("spark.repl.class.uri", null);
        if (classUri != null) {
            LOGGER.info("Using REPL class URI: {}", classUri);
            try {
                // TODO: ExecutorClassLoader实现
                Class<? extends ClassLoader> kclass = (Class<? extends ClassLoader>) classForName("org.apache.spark.repl.ExecutorClassLoader");
                Class<?>[] parameterTypes = new Class[]{SparkConf.class,
                                                        SparkEnv.class,
                                                        String.class,
                                                        ClassLoader.class,
                                                        boolean.class};
                Constructor<ClassLoader> constructor = (Constructor<ClassLoader>) kclass.getConstructor(parameterTypes);
                return constructor.newInstance(conf, env, classUri, parent, userClassPathFirst);
            } catch (Exception e) {
                LOGGER.error("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!");
                System.exit(1);
                return null;
            }
        }
        return parent;
    }

    public void stop() {
        try {
            heartbeater.shutdown();
            heartbeater.awaitTermination(10, TimeUnit.SECONDS);
            threadPool.shutdown();
            if (!isLocal) {
                env.stop();
            }
        } catch (InterruptedException e) {
            throw new SparkException("executor " + executorId + " shutdown failure", e);
        }
    }

    private class TaskRunner implements Runnable {

        private ExecutorBackend execBackend;

        private TaskDescription taskDescription;

        private String threadName;
        private long threadId;

        private long taskId;
        private String reasonIfKilled;
        private boolean finished = false;

        private Task task;
        private String taskName;

        // 任务运行是GC时间
        private volatile long startGCTime = 0L;

        public TaskRunner(ExecutorBackend execBackend, TaskDescription taskDescription) {
            this.execBackend = execBackend;
            this.taskDescription = taskDescription;
            this.taskId = this.taskDescription.taskId;
            this.taskName = this.taskDescription.name;
            this.threadName = String.format("Executor task launch worker for task %s", this.taskId);
        }

        public long getThreadId() {
            return threadId;
        }

        @Override
        public void run() {
            threadId = currentThread().getId();
            currentThread().setName(threadName);

            // Task运行耗时统计
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            // Task与TaskMemoryManager一一对应(即一个Task都会实例化一个TaskMemoryManager)
            TaskMemoryManager taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId);
            long deserializeStartTime = 0L;
            long deserializeStartCpuTime = 0L;
            if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
                deserializeStartCpuTime = threadMXBean.getCurrentThreadCpuTime();
            }

            currentThread().setContextClassLoader(replClassLoader);

            SerializerInstance ser = env.closureSerializer.newInstance();
            LOGGER.info("Running {} (TID {})", taskDescription.name, taskId);

            // 任务调度中心上报任务执行状态
            execBackend.statusUpdate(taskId, RUNNING, EMPTY_BYTE_BUFFER);

            long taskStart;
            long taskStartCpu = 0L;
            startGCTime = computeTotalGcTime();

            try {
                // Must be set before updateDependencies() is called, in case fetching dependencies
                // requires access to properties contained within (e.g. for access control).
                taskDeserializationProps.set(taskDescription.properties);
                // TODO: Task运行Jar包依赖
                updateDependencies(taskDescription.addedFiles, taskDescription.addedJars);

                task = ser.deserialize(taskDescription.serializedTask,
                                       currentThread().getContextClassLoader());
                task.localProperties = taskDescription.properties;
                task.setTaskMemoryManager(taskMemoryManager);

                if (reasonIfKilled != null) {
                    throw new TaskKilledException(reasonIfKilled);
                }

                // The purpose of updating the epoch here is to invalidate executor map output status cache
                // in case FetchFailures have occurred. In local mode `env.mapOutputTracker` will be
                // MapOutputTrackerMaster and its cache invalidation is not based on epoch numbers so
                // we don't need to make any special calls here.
                if (!isLocal) {
                    LOGGER.debug("Task {}'s epoch is {}", taskId, task.epoch);
                    MapOutputTrackerWorker mapOutputTracker = (MapOutputTrackerWorker) env.mapOutputTracker;
                    mapOutputTracker.updateEpoch(task.epoch);
                }

                // Task.run()
                taskStart = System.currentTimeMillis();
                if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
                    taskStartCpu = threadMXBean.getCurrentThreadCpuTime();
                }
                Object value;
                boolean threwException = true;
                try {
                    value = task.run(taskId, taskDescription.attemptNumber);
                    threwException = false;
                } finally {
                    // 释放TaskId对依赖BlockId所持有的读写锁
                    List<BlockId> releasedLocks = env.blockManager.releaseAllLocksForTask(taskId);
                    // 释放TaskId占用内存
                    long freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory();

                    if (freedMemory > 0 && !threwException) {
                        String errMsg = String.format("Managed memory leak detected; size = %d bytes, TID = %d",
                                                       freedMemory, taskId);
                        if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
                            throw new SparkException(errMsg);
                        } else {
                            LOGGER.warn(errMsg);
                        }
                    }

                    if (CollectionUtils.isNotEmpty(releasedLocks) && !threwException) {
                        String errMsg = String.format("%d block locks were not released by TID = %d: \n[%s]",
                                                      releasedLocks.size(), taskId, join(releasedLocks, ','));
                        if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
                            throw new SparkException(errMsg);
                        } else {
                            LOGGER.info(errMsg);
                        }
                    }
                }

                // 作业运行状态
                if (task.context.fetchFailed() != null) {
                    LOGGER.error("TID {} completed successfully though internally it encountered " +
                                 "unrecoverable fetch failures!  Most likely this means user code is incorrectly " +
                                 "swallowing Spark's internal {}", FetchFailedException.class, task.context.fetchFailed());
                }

                long taskFinish = System.currentTimeMillis();
                long taskFinishCpu = 0L;
                if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
                    taskFinishCpu = threadMXBean.getCurrentThreadCpuTime();
                }

                // If the task has been killed, let's fail it.
                task.context.killTaskIfInterrupted();

                SerializerInstance resultSer = env.closureSerializer.newInstance();
                long beforeSerialization = System.currentTimeMillis();
                ByteBuffer valueBytes = resultSer.serialize(value);
                long afterSerialization = System.currentTimeMillis();

                // TODO: TaskMetric
                long executorDeserializeTime = (taskStart - deserializeStartTime) + task.executorDeserializeTime;
                long executorDeserializeCpuTime = (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime;
                long executorRunTime = (taskFinish - taskStart) - task.executorDeserializeTime;
                long executorCpuTime = (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime;
                long executorGCTime = computeTotalGcTime() - startGCTime;
                long resultSerializationTime = afterSerialization - beforeSerialization;
                LOGGER.info("Task(TID = {}) take statistic: executorDeserializeTime = {}, executorDeserializeCpuTime = {}, " +
                            "executorRunTime = {}, executorCpuTime = {}, executorGCTime = {}, resultSerializationTime = {}",
                            taskId, executorDeserializeTime, executorDeserializeCpuTime, executorRunTime, executorCpuTime,
                            executorGCTime, resultSerializationTime);

                // directSend = sending directly back to the driver
                DirectTaskResult directResult = new DirectTaskResult(valueBytes);
                ByteBuffer serializedDirectResult = ser.serialize(directResult);
                int resultSize = serializedDirectResult.limit();

                ByteBuffer serializedResult;
                if (maxResultSize > 0 && resultSize > maxResultSize) {
                    LOGGER.warn("Finished {} (TID {}). Result is larger than maxResultSize ({} > {}),",
                                taskName, taskId, bytesToString(resultSize), bytesToString(maxResultSize));
                    serializedResult = ser.serialize(new IndirectTaskResult<>(null, new TaskResultBlockId(taskId), resultSize));
                } else if (resultSize > maxDirectResultSize) {
                    TaskResultBlockId blockId = new TaskResultBlockId(taskId);
                    env.blockManager.putBytes(blockId,
                                              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
                                              StorageLevel.MEMORY_AND_DISK_SER);
                    LOGGER.info("Finished {} (TID {}). {} bytes result sent via BlockManager",
                                taskName, taskId, resultSize);
                    serializedResult = ser.serialize(new IndirectTaskResult<>(null, blockId, resultSize));
                } else {
                    LOGGER.info("Finished {} (TID {}). {} bytes result sent to driver",
                                taskName, taskId, resultSize);
                    serializedResult = serializedDirectResult;
                }

                setTaskFinishedAndClearInterruptStatus();
                execBackend.statusUpdate(taskId, FINISHED, serializedResult);
            } catch (TaskKilledException e) {
                LOGGER.info("Executor killed {} (TID {}), reason: {}", taskName, taskId, e.getMessage());
                setTaskFinishedAndClearInterruptStatus();
                try {
                    execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(new TaskKilled(e.getMessage())));
                } catch (IOException ex) {
                    // ignore
                }
            } catch (Throwable e) {
                if (hasFetchFailure()) {
                    TaskFailedReason reason = task.context.fetchFailed().toTaskFailedReason();
                    if (!(e instanceof FetchFailedException)) {
                        String fetchFailedCls = FetchFailedException.class.getName();
                        LOGGER.warn("TID {} encountered a {} and failed, but the {} was hidden by another " +
                                "exception. Spark is handling this like a fetch failure and ignoring the " +
                                "other exception: {}", taskId, fetchFailedCls, e);
                    }
                    setTaskFinishedAndClearInterruptStatus();
                    try {
                        execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason));
                    } catch (IOException ex) {
                        // ignore
                    }
                } else {
                    LOGGER.error("Exception in {} (TID {})", taskName, taskId, e);
                    // TODO:
                }
            } finally {
                runningTasks.remove(taskId);
            }
        }

        public synchronized boolean isFinished() {
            return finished;
        }

        private boolean hasFetchFailure() {
            return task != null && task.context != null && task.context.fetchFailed() != null;
        }

        private synchronized void setTaskFinishedAndClearInterruptStatus() {
            this.finished = true;
            // SPARK-14234 - Reset the interrupted status of the thread to avoid the
            // ClosedByInterruptException during execBackend.statusUpdate which causes
            // Executor to crash
            Thread.interrupted();
            // Notify any waiting TaskReapers. Generally there will only be one reaper per task but there
            // is a rare corner-case where one task can have two reapers in case cancel(interrupt=False)
            // is followed by cancel(interrupt=True). Thus we use notifyAll() to avoid a lost wakeup:
            notifyAll();
        }

        private void kill(boolean interruptThread, String reason) {
            LOGGER.info("Executor is trying to kill {} (TID {}), reason: {}", taskName, taskId, reason);
            reasonIfKilled = reason;
            if (task != null) {
                synchronized (this) {
                    if (!finished) {
                        task.kill(interruptThread, reason);
                    }
                }
            }
        }

        // 更新Task需要Jar
        private void updateDependencies(Map<String, Long> newFiles, Map<String, Long> newJars) {
            throw new UnsupportedOperationException("");
        }
    }

    private class TaskReaper implements Runnable {

        private final TaskRunner taskRunner;
        private long taskId;
        private boolean interruptThread;
        private String reason;

        private long killPollingIntervalMs;
        private long killTimeoutMs;
        private boolean takeThreadDump;

        public TaskReaper(TaskRunner taskRunner, boolean interruptThread, String reason) {
            this.taskRunner = taskRunner;
            this.taskId = this.taskRunner.taskId;
            this.interruptThread = interruptThread;
            this.reason = reason;

            this.killPollingIntervalMs = conf.getTimeAsMs("spark.task.reaper.pollingInterval", "10s");
            this.killTimeoutMs = conf.getTimeAsMs("spark.task.reaper.killTimeout", "-1");
            this.takeThreadDump = conf.getBoolean("spark.task.reaper.threadDump", true);
        }

        private long elapsedTimeMs(long startTimeMs) {
            return System.currentTimeMillis() - startTimeMs;
        }

        private boolean timeoutExceeded(long elapsedTimeMs) {
            return killTimeoutMs > 0 && elapsedTimeMs > killTimeoutMs;
        }

        @Override
        public void run() {
            long startTimeMs = System.currentTimeMillis();
            try {
                // Only attempt to kill the task once. If interruptThread = false then a second kill
                // attempt would be a no-op and if interruptThread = true then it may not be safe or
                // effective to interrupt multiple times:
                taskRunner.kill(interruptThread, reason);
                // Monitor the killed task until it exits. The synchronization logic here is complicated
                // because we don't want to synchronize on the taskRunner while possibly taking a thread
                // dump, but we also need to be careful to avoid races between checking whether the task
                // has finished and wait()ing for it to finish.
                boolean finished = false;
                while (!finished && !timeoutExceeded(elapsedTimeMs(startTimeMs))) {
                    synchronized (taskRunner) {
                        if (taskRunner.isFinished()) {
                            finished = true;
                        } else {
                            try {
                                taskRunner.wait(killPollingIntervalMs);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }

                        // 等待超时或被TaskRunner唤醒
                        if (taskRunner.isFinished()) {
                            finished = true;
                        } else {
                            LOGGER.warn("Killed task {} is still running after {}ms", taskId, elapsedTimeMs(startTimeMs));
                            if (takeThreadDump) {
                                // TODO: Dump线程
                            }
                        }
                    }
                }

                if (!taskRunner.isFinished() && timeoutExceeded(startTimeMs)) {
                    if (isLocal) {
                        LOGGER.error("Killed task {} could not be stopped within {} ms; not killing JVM " +
                                "because we are running in local mode.", taskId, killTimeoutMs);
                    } else {
                        // In non-local-mode, the exception thrown here will bubble up to the uncaught exception
                        // handler and cause the executor JVM to exit.
                        throw new SparkException(String.format("Killing executor JVM because killed task %s " +
                                                               "could not be stopped within %s ms.", taskId, killTimeoutMs));
                    }
                }
            } finally {
                synchronized (taskReaperForTask) {
                    TaskReaper reaper = taskReaperForTask.get(taskId);
                    if (reaper != null && reaper.equals(this)) {
                        taskReaperForTask.remove(taskId);
                    }
                }
            }
        }
    }
}
