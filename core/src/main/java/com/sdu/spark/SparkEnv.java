package com.sdu.spark;

import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.sdu.spark.broadcast.BroadcastManager;
import com.sdu.spark.memory.MemoryManager;
import com.sdu.spark.memory.StaticMemoryManager;
import com.sdu.spark.memory.UnifiedMemoryManager;
import com.sdu.spark.network.BlockTransferService;
import com.sdu.spark.network.netty.NettyBlockTransferService;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.LiveListenerBus;
import com.sdu.spark.scheduler.OutputCommitCoordinator;
import com.sdu.spark.scheduler.OutputCommitCoordinatorEndPoint;
import com.sdu.spark.serializer.JavaSerializer;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.serializer.SerializerManager;
import com.sdu.spark.shuffle.ShuffleManager;
import com.sdu.spark.shuffle.sort.SortShuffleManager;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.storage.BlockManagerMaster;
import com.sdu.spark.storage.BlockManagerMasterEndpoint;
import com.sdu.spark.utils.Utils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.sdu.spark.security.CryptoStreamUtils.createKey;
import static com.sdu.spark.utils.RpcUtils.makeDriverRef;
import static com.sdu.spark.utils.Utils.classForName;
import static com.sdu.spark.utils.Utils.createTempDir;
import static com.sdu.spark.utils.Utils.deleteRecursively;
import static org.apache.commons.lang3.BooleanUtils.toBoolean;

/**
 * @author hanhan.zhang
 * */
public class SparkEnv {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkEnv.class);

    /**每个JVM进程中只有一个SparkContext*/
    public static volatile SparkEnv env;

    private static final String driverSystemName = "sparkDriver";
    private static final String executorSystemName = "sparkExecutor";

    private String driverTmpDir;
    private boolean isStopped = false;
    private Map<String, Object> hadoopJobMetadata = new MapMaker().weakValues().makeMap();

    private String executorId;
    public RpcEnv rpcEnv;
    public SparkConf conf;
    public ShuffleManager shuffleManager;
    private BroadcastManager broadcastManager;
    private SerializerManager serializerManager;
    private OutputCommitCoordinator outputCommitCoordinator;
    public BlockManager blockManager;
    public MemoryManager memoryManager;
    public Serializer closureSerializer;
    public MapOutputTracker mapOutputTracker;


    public SparkEnv(String executorId,
                    RpcEnv rpcEnv,
                    Serializer closureSerializer,
                    MapOutputTracker mapOutputTracker,
                    ShuffleManager shuffleManager,
                    BroadcastManager broadcastManager,
                    BlockManager blockManager,
                    SerializerManager serializerManager,
                    MemoryManager memoryManager,
                    OutputCommitCoordinator outputCommitCoordinator,
                    SparkConf conf) {
        this.executorId = executorId;
        this.rpcEnv = rpcEnv;
        this.mapOutputTracker = mapOutputTracker;
        this.shuffleManager = shuffleManager;
        this.broadcastManager = broadcastManager;
        this.serializerManager = serializerManager;
        this.outputCommitCoordinator = outputCommitCoordinator;
        this.conf = conf;
        this.blockManager = blockManager;
        this.memoryManager = memoryManager;

        this.closureSerializer = closureSerializer;
    }

    public void stop() {
        if (!isStopped) {
            isStopped = true;
            mapOutputTracker.stop();
            shuffleManager.stop();
            broadcastManager.stop();
            blockManager.stop();
            blockManager.master.stop();
            outputCommitCoordinator.stop();
            rpcEnv.shutdown();
            rpcEnv.awaitTermination();

            // If we only stop sc, but the driver process still run as a services then we need to delete
            // the tmp dir, if not, it will create too many tmp dirs.
            // We only need to delete the tmp dir create by driver
            if (driverTmpDir != null) {
                try {
                    deleteRecursively(new File(driverTmpDir));
                } catch (Exception e) {
                    LOGGER.warn("Exception while deleting Spark temp dir: {}", driverTmpDir, e);
                }
            }
        }
    }

    public static SparkEnv createDriverEnv(SparkConf conf,
                                           boolean isLocal,
                                           LiveListenerBus listenerBus,
                                           int numCores,
                                           OutputCommitCoordinator mockOutputCommitCoordinator) {
        assert conf.contains("spark.driver.host") : "Spark Driver host is not set !";
        String bindAddress = conf.get("spark.driver.bindAddress");
        String advertiseAddress = conf.get("spark.driver.host");
        int port = NumberUtils.toInt(conf.get("spark.driver.port"));
        byte[] ioEncryptionKey = null;
        boolean isEncryption = toBoolean(conf.get("spark.io.encryption.enabled"));
        if (isEncryption) {
            ioEncryptionKey = createKey(conf);
        }

        return create(
                conf,
                SparkContext.DRIVER_IDENTIFIER,
                bindAddress,
                advertiseAddress,
                port,
                isLocal,
                numCores,
                ioEncryptionKey,
                listenerBus,
                mockOutputCommitCoordinator);
    }

    public static SparkEnv createExecutorEnv(SparkConf conf,
                                             String executorId,
                                             String hostname,
                                             int numCores,
                                             byte[] ioEncryptionKey,
                                             boolean isLocal) {
        SparkEnv env = create(conf, executorId, hostname, hostname, 0, isLocal,
                              numCores, ioEncryptionKey, null, null);
        SparkEnv.env = env;
        return env;
    }

    private static SparkEnv create(SparkConf conf,
                                   String executorId,
                                   String bindAddress,
                                   String advertiseAddress,
                                   int port,
                                   boolean isLocal,
                                   int numUsableCores,
                                   byte[] ioEncryptionKey,
                                   LiveListenerBus liveListenerBus,
                                   OutputCommitCoordinator mockOutputCommitCoordinator) {
        boolean isDriver = executorId.equals(SparkContext.DRIVER_IDENTIFIER);

        // 事件监听只在Driver端处理
        if (isDriver) {
            checkArgument(liveListenerBus != null, "Attempted to create driver SparkEnv with null listener bus!");
        }

        // 安全认证
        SecurityManager securityManager = new SecurityManager(conf, ioEncryptionKey);
        if (!securityManager.isEncryptionEnabled()) {
            LOGGER.warn("I/O encryption enabled without RPC encryption: keys will be visible on the wire.");
        }

        // RPC通信
        String systemName = isDriver ? driverSystemName : executorSystemName;
        RpcEnv rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port, conf,
                                      securityManager, numUsableCores, !isDriver);
        if (isDriver) {
            conf.set("spark.driver.port", String.valueOf(rpcEnv.address().port));
        }

        // RPC消息序列化
        Serializer serializer = instantiateClassFromConf(conf,
                                                         isDriver,
                                                         "spark.closureSerializer",
                                                         "com.sdu.spark.closureSerializer.JavaSerializer");
        LOGGER.debug("Using closureSerializer: {}", serializer.getClass());
        SerializerManager serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey);
        JavaSerializer closureSerializer = new JavaSerializer(conf);

        // 广播组件
        BroadcastManager broadcastManager = new BroadcastManager(isDriver, conf, securityManager);

        // Map输出管理
        MapOutputTracker mapOutputTracker = isDriver ? new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
                                                     : new MapOutputTrackerWorker(conf);
        MapOutputTrackerRpcEndPointCreator creator = tracker -> new MapOutputTrackerMasterEndPoint(rpcEnv, tracker, conf);
        mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(conf, isDriver, rpcEnv,
                                                                    MapOutputTracker.ENDPOINT_NAME,
                                                                    mapOutputTracker, creator);

        // Shuffle管理
        Map<String, String> shortShuffleMgrNames = Maps.newHashMap();
        shortShuffleMgrNames.put("sort", SortShuffleManager.class.getName());
        shortShuffleMgrNames.put("tungsten-sort", SortShuffleManager.class.getName());
        String shuffleMgrName = shortShuffleMgrNames.get(conf.get("spark.shuffle.manager", "sort"));
        ShuffleManager shuffleManager = instantiateClass(conf, isDriver, shuffleMgrName);

        // Block Storage管理
        // step1: Block Storage Memory内存管理
        boolean useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false);
        MemoryManager memoryManager = useLegacyMemoryManager ? new StaticMemoryManager(conf, numUsableCores)
                                                             : new UnifiedMemoryManager(conf, numUsableCores);
        // step2: Block传输/获取
        int blockManagerPort = isDriver ? conf.getInt("spark.driver.blockManager.port", 0)
                                        : conf.getInt("spark.blockManager.port", 0);
        BlockTransferService blockTransferService = new NettyBlockTransferService(
                conf,
                securityManager,
                bindAddress,
                port,
                numUsableCores
        );
        // step3: Block Manager Master RpcEndPoint(负责接收消息, 获取Block数据、注册及状态等)
        GeneralRpcEndPointCreator rpcEndPointCreator = () -> new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, liveListenerBus);
        BlockManagerMaster blockManagerMaster = new BlockManagerMaster(
                registerOrLookupEndpoint(conf, isDriver, rpcEnv, BlockManagerMaster.DRIVER_ENDPOINT_NAME, rpcEndPointCreator),
                conf,
                isDriver
        );
        // step4: 创建BlockManager(Executor、Driver都创建且BlockManager只有initialize()方可有效)
        BlockManager blockManager = new BlockManager(
                executorId,
                rpcEnv,
                blockManagerMaster,
                serializerManager,
                conf,
                memoryManager,
                mapOutputTracker,
                shuffleManager,
                blockTransferService,
                securityManager,
                numUsableCores
        );

        // TODO: Spark Metric System

        OutputCommitCoordinator outputCommitCoordinator = mockOutputCommitCoordinator == null ? new OutputCommitCoordinator(conf, isDriver)
                                                                                              : mockOutputCommitCoordinator;
        GeneralRpcEndPointCreator outputCommitRpcEndPointCreator = () -> new OutputCommitCoordinatorEndPoint(rpcEnv, outputCommitCoordinator);
        outputCommitCoordinator.coordinatorRef = registerOrLookupEndpoint(
                conf,
                isDriver,
                rpcEnv,
                "OutputCommitCoordinator",
                outputCommitRpcEndPointCreator
        );

        SparkEnv envInstance = new SparkEnv(
                executorId,
                rpcEnv,
                serializer,
                mapOutputTracker,
                shuffleManager,
                broadcastManager,
                blockManager,
                serializerManager,
                memoryManager,
                outputCommitCoordinator,
                conf
        );

        // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
        // called, and we only need to do it for driver. Because driver may run as a service, and if we
        // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
        if (isDriver) {
            try {
                envInstance.driverTmpDir = createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath();
            } catch (IOException e) {
                throw new SparkException(e);
            }
        }
        return envInstance;
    }

    private static <T> T instantiateClass(SparkConf conf, boolean isDriver, String className) {
        Class<?> cls = classForName(className);
        try {
            return (T) cls.getConstructor(SparkConf.class, Boolean.TYPE)
                      .newInstance(conf, isDriver);
        } catch (Exception e) {
            try {
                return (T) cls.getConstructor(SparkConf.class)
                        .newInstance(conf);
            } catch (Exception ex) {
                try {
                    return (T) cls.getConstructor().newInstance();
                } catch (Exception ey) {
                    throw new SparkException("Instance class " + className + " failure", e);
                }
            }
        }
    }

    private static <T> T instantiateClassFromConf(SparkConf conf, boolean isDriver,
                                                  String propertyName, String defaultClassName) {
        return instantiateClass(conf, isDriver, conf.get(propertyName, defaultClassName));
    }

    private static RpcEndPointRef registerOrLookupEndpoint(SparkConf conf, boolean isDriver, RpcEnv rpcEnv,
                                                           String name, MapOutputTracker mapOutputTracker, MapOutputTrackerRpcEndPointCreator creator) {
        if (isDriver) {
            LOGGER.info("Registering {}", name);
            return rpcEnv.setRpcEndPointRef(name, creator.create((MapOutputTrackerMaster) mapOutputTracker));
        } else {
            return makeDriverRef(name, conf, rpcEnv);
        }
    }

    private static RpcEndPointRef registerOrLookupEndpoint(SparkConf conf, boolean isDriver, RpcEnv rpcEnv,
                                                           String name, GeneralRpcEndPointCreator creator) {
        if (isDriver) {
            LOGGER.info("Registering {}", name);
            return rpcEnv.setRpcEndPointRef(name, creator.create());
        } else {
            return makeDriverRef(name, conf, rpcEnv);
        }
    }

    private interface MapOutputTrackerRpcEndPointCreator {
        RpcEndPoint create(MapOutputTrackerMaster tracker);
    }

    private interface GeneralRpcEndPointCreator {
        RpcEndPoint create();
    }
}
