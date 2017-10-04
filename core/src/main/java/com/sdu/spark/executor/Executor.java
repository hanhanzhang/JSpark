package com.sdu.spark.executor;

import com.google.common.collect.Maps;
import com.sdu.spark.HeartBeatReceiver;
import com.sdu.spark.MapOutputTrackerWorker;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.TaskKilledException;
import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.DirectTaskResult;
import com.sdu.spark.scheduler.IndirectTaskResult;
import com.sdu.spark.scheduler.Task;
import com.sdu.spark.scheduler.TaskDescription;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockId.TaskResultBlockId;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.ChunkedByteBuffer;
import com.sdu.spark.utils.SparkUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.sdu.spark.scheduler.TaskState.FINISHED;
import static com.sdu.spark.scheduler.TaskState.RUNNING;
import static com.sdu.spark.utils.RpcUtils.makeDriverRef;
import static com.sdu.spark.utils.ThreadUtils.newDaemonCachedThreadPool;
import static com.sdu.spark.utils.ThreadUtils.newDaemonSingleThreadScheduledExecutor;
import static com.sdu.spark.utils.Utils.computeTotalGcTime;

/**
 * {@link Executor}Spark Task执行器,
 *
 * @author hanhan.zhang
 * */
public class Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Executor.class);
    private ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

    /****************************Spark运行环境*******************************/
    private boolean isLocal;
    private String executorId;
    private String executorHostname;
    private SparkEnv env;
    private SparkConf conf;
    private URL[] userClassPath;

    /****************************Spark Task*********************************/
    // 任务线程
    private ThreadPoolExecutor threadPool = newDaemonCachedThreadPool("Executor task launch worker-%d", Integer.MAX_VALUE, 60);
    // 任务取消线程
    private ThreadPoolExecutor taskReaperPool = newDaemonCachedThreadPool("Task reaper");
    // 运行中任务集合[key = taskId, value = TaskRunner]
    private Map<Long, TaskRunner> runningTasks = Maps.newConcurrentMap();
    // 向Driver发现心跳
    private ScheduledExecutorService heartbeater = newDaemonSingleThreadScheduledExecutor("driver-heartbeater");
    private RpcEndPointRef heartbeatReceiverRef;
    private int heartbeatFailures = 0;
    private int HEARTBEAT_MAX_FAILURES;

    /****************************Spark Task Result**************************/
    private int maxResultSize;
    // Max size of direct result. If task result is bigger than this, we use the block manager
    // to send the result back.
    private int maxDirectResultSize;

    /*************************Spark Thread Exception Handler****************/
    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    private static ThreadLocal<Properties> taskDeserializationProps = new ThreadLocal<>();

    public Executor(String executorId, String executorHostname, SparkEnv env, URL[] userClassPath) {
        this(executorId, executorHostname, env, false, userClassPath, new SparkUncaughtExceptionHandler());
    }

    public Executor(String executorId, String executorHostname, SparkEnv env, boolean isLocal,
                    URL[] userClassPath, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.executorId = executorId;
        this.executorHostname = executorHostname;
        this.env = env;
        this.conf = env.conf;
        this.isLocal = isLocal;
        this.userClassPath = userClassPath;
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60);
        maxResultSize = conf.getInt("spark.driver.maxResultSize", 1024);
        maxDirectResultSize = conf.getInt("spark.task.maxDirectResultSize", 10240);
        heartbeatReceiverRef = makeDriverRef(HeartBeatReceiver.ENDPOINT_NAME, conf, this.env.rpcEnv);
        if (!isLocal) {
            this.env.blockManager.initialize(this.conf.getAppId());
        }
        startDriverHeartbeat();
    }

    /*****************************定时向Driver上报心跳*************************/
    private void startDriverHeartbeat() {
        long intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval=", "10s");
        long initialDelay = (long) (intervalMs + Math.random() * intervalMs);
        heartbeater.scheduleAtFixedRate(this::reportHeartBeat, initialDelay, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void reportHeartBeat() {
        // TODO: 统计信息尚未上报
        Heartbeat message = new Heartbeat(executorId, env.blockManager.blockManagerId);
        try {
            HeartbeatResponse response = (HeartbeatResponse) heartbeatReceiverRef.askSync(message, conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s"));
            if (response.registerBlockManager) {
                env.blockManager.reregister();
            }
        } catch (Exception e) {
            heartbeatFailures += 1;
            if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
                LOGGER.error("超过{}次未收到Driver(hostPort = {}:{})的心跳响应, Executor(execId = {})退出",
                            heartbeatFailures,
                            conf.get("spark.driver.host", "localhost"), conf.get("spark.driver.port", "7077"),
                            executorId);
                System.exit(56);
            }
        }

    }

    /******************************Executor执行Task****************************/
    public void launchTask(ExecutorBackend context, TaskDescription taskDescription) {
        TaskRunner tr = new TaskRunner(context, taskDescription);
        runningTasks.put(taskDescription.taskId, tr);
        threadPool.execute(tr);
    }


    /******************************Executor杀死Task***************************/
    public void killTask(long taskId, boolean interruptThread, String reason) {
        TaskRunner taskRunner = runningTasks.get(taskId);
        if (taskRunner != null) {

        }
    }

    public void killAllTasks(boolean interruptThread, String reason) {
        runningTasks.keySet().forEach(taskId -> killTask(taskId, interruptThread, reason));
    }

    public void stop() throws InterruptedException {
        heartbeater.shutdown();
        heartbeater.awaitTermination(10, TimeUnit.SECONDS);
        threadPool.shutdown();
        if (!isLocal) {
            env.stop();
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

        // 任务运行是GC时间
        private volatile long startGCTime = 0L;

        public TaskRunner(ExecutorBackend execBackend, TaskDescription taskDescription) {
            this.execBackend = execBackend;
            this.taskDescription = taskDescription;
            this.taskId = this.taskDescription.taskId;
            this.threadName = String.format("Executor task launch worker for task %s", this.taskId);
        }

        @Override
        public void run() {
            threadId = Thread.currentThread().getId();
            Thread.currentThread().setName(threadName);

            /**
             * Task运行耗时统计
             * */
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            long deserializeStartTime = 0L;
            long deserializeStartCpuTime = 0L;
            // JVM若是支持测量CPU耗时, 则获取CPU初始耗时
            if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
                deserializeStartCpuTime = threadMXBean.getCurrentThreadCpuTime();
            }

            TaskMemoryManager taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId);
            SerializerInstance ser = env.serializer.newInstance();
            LOGGER.info("执行Task(name = {}, taskId = {})", taskDescription.name, taskId);
            /**
             * 向任务调度中心上报任务执行状态
             * */
            execBackend.statusUpdate(taskId, RUNNING, EMPTY_BYTE_BUFFER);

            long taskStart = 0L;
            long taskStartCpu = 0L;
            startGCTime = computeTotalGcTime();

            try {
                // Task运行环境
                taskDeserializationProps.set(taskDescription.properties);
                updateDependencies(taskDescription.addedFiles, taskDescription.addedJars);

                // 序列化运行Task
                task = ser.deserialize(taskDescription.serializedTask, Thread.currentThread().getContextClassLoader());
                task.localProperties = taskDescription.properties;
                task.taskMemoryManager = taskMemoryManager;

                if (reasonIfKilled != null) {
                    throw new TaskKilledException(reasonIfKilled);
                }

                if (!isLocal) {
                    MapOutputTrackerWorker mapOutputTracker = (MapOutputTrackerWorker) env.mapOutputTracker;
                    mapOutputTracker.updateEpoch(task.epoch);

                }

                // 任务运行
                taskStart = System.currentTimeMillis();
                if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
                    taskStartCpu = threadMXBean.getCurrentThreadCpuTime();
                }

                Object value;
                try {
                    value = task.run(taskId, taskDescription.attemptNumber);
                } finally {
                    /**
                     * 释放资源
                     * */
                    List<BlockId> releasedLocks = env.blockManager.releaseAllLocksForTask(taskId);
                    long freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory();
                    if (freedMemory > 0) {
                        LOGGER.info("内存发生泄漏: size = {}, TID = {}", freedMemory, taskId);
                    }
                    if (releasedLocks != null && releasedLocks.size() > 0) {
                        LOGGER.info("锁没有释放: size = {}, TID = {}", releasedLocks.size(), taskId);
                    }
                }

                long taskFinish = System.currentTimeMillis();
                long taskFinishCpu = 0L;
                if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
                    taskFinishCpu = threadMXBean.getCurrentThreadCpuTime();
                }

                // If the task has been killed, let's fail it.
                task.context.killTaskIfInterrupted();

                /**
                 * 序列化结果
                 * */
                SerializerInstance resultSer = env.serializer.newInstance();
                long beforeSerialization = System.currentTimeMillis();
                ByteBuffer valueBytes = resultSer.serialize(value);
                long afterSerialization = System.currentTimeMillis();

                /**
                 * 任务运行耗时统计
                 * */
                long executorDeserializeTime = (taskStart - deserializeStartTime) + task.executorDeserializeTime;
                long executorDeserializeCpuTime = (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime;
                long executorRunTime = (taskFinish - taskStart) - task.executorDeserializeTime;
                long executorCpuTime = (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime;
                long executorGCTime = computeTotalGcTime() - startGCTime;
                long resultSerializationTime = afterSerialization - beforeSerialization;
                LOGGER.info("Task(TID = {})耗时统计: executorDeserializeTime = {}, executorDeserializeCpuTime = {}, " +
                        "executorRunTime = {}, executorCpuTime = {}, executorGCTime = {}, resultSerializationTime = {}",
                        taskId, executorDeserializeTime, executorDeserializeCpuTime, executorRunTime, executorCpuTime,
                        executorGCTime, resultSerializationTime);

                /**
                 * 向Driver发送运行结果
                 * */
                DirectTaskResult directResult = new DirectTaskResult(valueBytes);
                ByteBuffer serializedDirectResult = ser.serialize(directResult);
                int resultSize = serializedDirectResult.limit();

                ByteBuffer serializedResult;
                if (maxResultSize > 0 && resultSize > maxResultSize) {
                    LOGGER.info("Task(TID = {})运行完成, 由于运行结果超出最大结果限制而丢弃: maxResultSize = {}, " +
                            "taskResultSize = {}", taskId, maxResultSize, resultSize);
                    serializedResult = ser.serialize(new IndirectTaskResult<>(null, new TaskResultBlockId(taskId), resultSize));
                } else if (resultSize > maxDirectResultSize) {
                    TaskResultBlockId blockId = new TaskResultBlockId(taskId);
                    env.blockManager.putBytes(blockId,
                            new ChunkedByteBuffer(serializedDirectResult.duplicate()),
                            StorageLevel.MEMORY_AND_DISK_SER);
                    LOGGER.info("Task(TID = {})运行完成, 运行结果由BlockManager发送: taskResultSize = {}", taskId, resultSize);
                    serializedResult = ser.serialize(new IndirectTaskResult<>(null, blockId, resultSize));
                } else {
                    LOGGER.info("Task(TID = {})运行完成, 运行结果发送给Driver: taskResultSize = {}", taskId, resultSize);
                    serializedResult = serializedDirectResult;
                }

                setTaskFinishedAndClearInterruptStatus();
                execBackend.statusUpdate(taskId, FINISHED, serializedResult);
            } catch (IOException e) {

            } catch (TaskKilledException e) {

            } finally {
                runningTasks.remove(taskId);
            }
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

        private void killTask() {

        }

        // 更新Task需要Jar
        private void updateDependencies(Map<String, Long> newFiles, Map<String, Long> newJars) {
            throw new UnsupportedOperationException("");
        }
    }
}
