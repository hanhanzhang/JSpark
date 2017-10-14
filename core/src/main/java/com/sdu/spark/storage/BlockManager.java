package com.sdu.spark.storage;

import com.google.common.collect.Sets;
import com.sdu.spark.MapOutputTracker;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.SparkContext;
import com.sdu.spark.SparkException;
import com.sdu.spark.memory.MemoryManager;
import com.sdu.spark.memory.MemoryMode;
import com.sdu.spark.network.BlockDataManager;
import com.sdu.spark.network.BlockTransferService;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.shuffle.ExternalShuffleClient;
import com.sdu.spark.network.shuffle.ShuffleClient;
import com.sdu.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import com.sdu.spark.network.utils.TransportConf;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.serializer.SerializerManager;
import com.sdu.spark.shuffle.ShuffleManager;
import com.sdu.spark.storage.BlockData.Allocator;
import com.sdu.spark.storage.BlockData.ByteBufferBlockData;
import com.sdu.spark.storage.BlockManagerInfo.BlockStatus;
import com.sdu.spark.storage.memory.BlockEvictionHandler;
import com.sdu.spark.storage.memory.MemoryStore;
import com.sdu.spark.unfase.Platform;
import com.sdu.spark.utils.ChunkedByteBuffer;
import com.sdu.spark.utils.IdGenerator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static com.sdu.spark.network.netty.SparkTransportConf.fromSparkConf;
import static com.sdu.spark.utils.ThreadUtils.newDaemonCachedThreadPool;
import static com.sdu.spark.utils.Utils.classForName;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

/**
 *
 *
 * @author hanhan.zhang
 * */
public class BlockManager implements BlockDataManager, BlockEvictionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockManager.class);
    private static final IdGenerator ID_GENERATOR = new IdGenerator();

    private String executorId;
    private RpcEnv rpcEnv;
    public BlockManagerMaster master;
    private SerializerManager serializerManager;
    private SparkConf conf;
    public final MemoryManager memoryManager;
    private MapOutputTracker mapOutputTracker;
    private ShuffleManager shuffleManager;
    private BlockTransferService blockTransferService;
    private SecurityManager securityManager;
    private int numUsableCores;

    private boolean externalShuffleServiceEnabled;

    public BlockManagerId blockManagerId;

    // Shuffle Block数据块存储服务地址、数据块存储服务客户端、数据块存储服务端口
    private BlockManagerId shuffleServerId;
    private ShuffleClient shuffleClient;
    private int externalShuffleServicePort;

    private long maxFailuresBeforeLocationRefresh;

    private RpcEndPointRef slaveEndpoint;

    private BlockInfoManager blockInfoManager;

    // 存储管理
    public DiskBlockManager diskBlockManager;

    // Block实际存储
    private DiskStore diskStore;
    public MemoryStore memoryStore;

    private BlockReplicationPolicy blockReplicationPolicy;
    private volatile Set<BlockManagerId> cachedPeers;
    private final Object peerFetchLock = new Object();
    private long lastPeerFetchTime = 0L;

    private long maxOffHeapMemory;
    private long maxOnHeapMemory;

    private ThreadPoolExecutor futureExecutionContext;
    private Future<?> asyncReregisterTask = null;
    private final Object asyncReregisterLock = new Object();

    public BlockManager(String executorId, RpcEnv rpcEnv, BlockManagerMaster master,
                        SerializerManager serializerManager, SparkConf conf, MemoryManager memoryManager,
                        MapOutputTracker mapOutputTracker, ShuffleManager shuffleManager,
                        BlockTransferService blockTransferService, SecurityManager securityManager,
                        int numUsableCores) {
        this.executorId = executorId;
        this.rpcEnv = rpcEnv;
        this.master = master;
        this.serializerManager = serializerManager;
        this.conf = conf;
        this.memoryManager = memoryManager;
        this.mapOutputTracker = mapOutputTracker;
        this.shuffleManager = shuffleManager;
        this.blockTransferService = blockTransferService;
        this.securityManager = securityManager;
        this.numUsableCores = numUsableCores;


        this.externalShuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false);

        boolean deleteShuffleBlockFile = false;
        if (!this.externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER) {
            // Shuffle Block存储不是外部服务, 则需要退出时删除Block落地磁盘文件
            deleteShuffleBlockFile = true;
        }
        this.diskBlockManager = new DiskBlockManager(conf, deleteShuffleBlockFile);
        this.blockInfoManager = new BlockInfoManager();

        // Block数据块实际存储位置
        this.diskStore = new DiskStore(conf, this.diskBlockManager, securityManager);
        this.memoryStore = new MemoryStore(conf, this.blockInfoManager, serializerManager, memoryManager, this);
        // Block分配内存容量
        this.maxOnHeapMemory = memoryManager.maxOnHeapStorageMemory();
        this.maxOffHeapMemory = memoryManager.maxOffHeapStorageMemory();

        // Block数据存储服务客户端
        // TODO: Shuffle Block 存储Hadoop
        this.externalShuffleServicePort = toInt(conf.get("spark.shuffle.service.port"));
        if (this.externalShuffleServiceEnabled) {
            TransportConf transportConf = fromSparkConf(conf, "shuffle", numUsableCores);
            this.shuffleClient = new ExternalShuffleClient(transportConf, securityManager,
                                        securityManager.isAuthenticationEnabled(), conf.getTimeAsMs("spark.shuffle.registration.timeout", "5000"));
        } else {
            this.shuffleClient = blockTransferService;
        }
        this.maxFailuresBeforeLocationRefresh = conf.getInt("spark.block.failures.beforeLocationRefresh", 5);

        // 接受来自BlockManagerMasterEndPoint网络消息
        String endPointName = "BlockManagerEndpoint" + ID_GENERATOR.next();
        this.slaveEndpoint = rpcEnv.setRpcEndPointRef(endPointName, new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker));

        // 异步注册线程池
        futureExecutionContext = newDaemonCachedThreadPool("block-manager-future", 128, 60);
    }


    public void initialize(String appId) {
        blockTransferService.init(appId);
        shuffleClient.init(appId);

        // Block副本策略初始化
        try {
            String priorityClass = conf.get(
                    "spark.storage.replication.policy", RandomBlockReplicationPolicy.class.getName());
            Class<?> clazz = classForName(priorityClass);
            this.blockReplicationPolicy = (BlockReplicationPolicy) clazz.newInstance();
            LOGGER.info("Using {} for block replication policy", priorityClass);
        } catch (Exception e) {
            throw new SparkException("initialize block replication policy failure", e);
        }

        // 初始化Block存储地址信息及向BlockManagerMasterEndpoint注册BlockManagerId
        // Executor或Driver进程启动的BlockManager
        BlockManagerId id = new BlockManagerId(executorId, blockTransferService.hostName(), blockTransferService.port(), "");
        BlockManagerId idFromMaster = master.registerBlockManager(id, maxOnHeapMemory, maxOffHeapMemory, slaveEndpoint);
        this.blockManagerId = idFromMaster == null ? id : idFromMaster;

        // 初始化Shuffle Block外部存储服务地址并注册
        if (externalShuffleServiceEnabled) {
            LOGGER.info("external shuffle service port = {}", externalShuffleServicePort);
            this.shuffleServerId = BlockManagerId.apply(executorId, blockTransferService.hostName(), externalShuffleServicePort, "");
        } else {
            this.shuffleServerId = this.blockManagerId;
        }
        if (externalShuffleServiceEnabled && !blockManagerId.isDriver()) {
            registerWithExternalShuffleServer();
        }
        LOGGER.info("Initialized BlockManager: {}", blockManagerId);
    }

    private void registerWithExternalShuffleServer() {
        LOGGER.info("Registering executor with local external shuffle service.");
        String[] localDirPath = new String[diskBlockManager.localDirs.length];
        for (int i = 0; i < localDirPath.length; ++i) {
            localDirPath[i] = diskBlockManager.localDirs[i].toString();
        }
        ExecutorShuffleInfo executorShuffleInfo = new ExecutorShuffleInfo(localDirPath,
                                                            diskBlockManager.subDirsPerLocalDir,
                                                            shuffleManager.getClass().getName());

        int MAX_ATTEMPTS = conf.getInt("spark.shuffle.registration.maxAttempts", 5);
        int SLEEP_TIME_SECS = 5;

        for (int i = 0; i < MAX_ATTEMPTS; ++i) {
            try {
                ExternalShuffleClient externalShuffleClient = (ExternalShuffleClient) shuffleClient;
                // 向BlockTransportServer注册服务信息
                externalShuffleClient.registerWithShuffleServer(blockTransferService.hostName(),
                                                                blockTransferService.port(),
                                                                executorId, executorShuffleInfo);
            } catch (Exception e) {
                if (i < MAX_ATTEMPTS) {
                    try {
                        Thread.sleep(SLEEP_TIME_SECS * 1000L);
                    } catch (InterruptedException e1) {
                        // ingore
                    }
                }
            }
        }
    }

    private void reportAllBlocks() {
        LOGGER.info("Reporting {} blocks to the master.", blockInfoManager.size());
        for (Map.Entry<BlockId, BlockInfo> entries : blockInfoManager.entries()) {
            BlockId blockId = entries.getKey();
            BlockInfo blockInfo = entries.getValue();
            BlockStatus blockStatus = getCurrentBlockStatus(blockId, blockInfo);
            if (blockInfo.tellMaster && !tryToReportBlockStatus(blockId, blockStatus, 0)) {
                LOGGER.error("Failed to report {} to master; giving up.", blockId);
                return;
            }
        }
    }

    public List<BlockId> releaseAllLocksForTask(long taskId) {
        return blockInfoManager.releaseAllLocksForTask(taskId);
    }


    public <T> Pair<BlockResult, Iterator<T>> getOrElseUpdate(BlockId blockId,
                                                              StorageLevel storageLevel,
                                                              RDDIterator<T> rddIterator) {
        // TODO: 待实现
        throw new UnsupportedOperationException("");
    }

    private boolean doPutBytes(BlockId blockId, ChunkedByteBuffer bytes,
                                   StorageLevel level, boolean tellMaster) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public ManagedBuffer getBlockData(BlockId blockId) {
        if (blockId.isShuffle()) {
            // TODO: Shuffle Manager负责Block数据请求
        } else {
            BlockData data = getLocalBytes(blockId);
            if (data != null) {
                return new BlockManagerManagedBuffer(blockInfoManager, blockId, data, true);
            }
        }
        // If this block manager receives a request for a block that it doesn't have then it's
        // likely that the master has outdated block statuses for this block. Therefore, we send
        // an RPC so that this block is marked as being unavailable from this block manager.
        reportBlockStatus(blockId, BlockStatus.empty(), 0);
        throw new BlockNotFoundException(blockId.toString());
    }

    @Override
    public boolean putBlockData(BlockId blockId, ManagedBuffer data, StorageLevel level) {
        return false;
    }

    public void registerTask(long taskAttemptId) {
        blockInfoManager.registerTask(taskAttemptId);
    }

    @Override
    public void releaseLock(BlockId blockId, long taskAttemptId) {
        blockInfoManager.unlock(blockId, taskAttemptId);
    }

    private void reportBlockStatus(BlockId blockId, BlockStatus status, long droppedMemorySize) {
        boolean needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize);
        if (needReregister) {
            LOGGER.info("Got told to re-register updating block {}", blockId);
            asyncReregister();
        }
        LOGGER.debug("Told master about block {}", blockId);
    }

    private boolean tryToReportBlockStatus(BlockId blockId, BlockStatus blockStatus, long droppedMemorySize) {
        // 向BlockManagerMasterEndpoint汇报Block状态
        long inMemSize = Math.max(blockStatus.memorySize, droppedMemorySize);
        long onDiskSize = blockStatus.diskSize;
        return master.updateBlockInfo(blockManagerId, blockId, blockStatus.storageLevel,
                                      inMemSize, onDiskSize);
    }

    private void asyncReregister() {
        synchronized (asyncReregisterLock) {
            if (asyncReregisterTask == null) {
                asyncReregisterTask = futureExecutionContext.submit(this::reregister);
                asyncReregisterTask = null;
            }
        }
    }

    public void reregister() {
        LOGGER.info("BlockManager {} re-registering with master", blockManagerId);
        master.registerBlockManager(blockManagerId, maxOnHeapMemory, maxOffHeapMemory, slaveEndpoint);
        reportAllBlocks();
    }

    private BlockStatus getCurrentBlockStatus(BlockId blockId, BlockInfo blockInfo) {
        StorageLevel level = blockInfo.storageLevel;
        if (level == null) {
            return BlockStatus.empty();
        }
        // 只能存在内存或磁盘
        boolean inMemory = level.useMemory && memoryStore.contains(blockId);
        boolean inDisk = level.useDisk && diskStore.contains(blockId);
        boolean deserialized = inMemory && level.deserialized;
        int replication = inMemory || inDisk ? level.replication : 1;

        StorageLevel storageLevel = StorageLevel.apply(inDisk, inMemory, level.useOffHeap, deserialized, replication);

        long memSize = inMemory ? memoryStore.getSize(blockId) : 0L;
        long diskSize = inDisk ? diskStore.getSize(blockId) : 0L;
        return new BlockStatus(storageLevel, memSize, diskSize);
    }

    private BlockData getLocalBytes(BlockId blockId) {
        LOGGER.debug("Getting local block {} as bytes", blockId);
        if (blockId.isShuffle()) {
            // TODO: Shuffle Manager负责Block数据获取
            throw new UnsupportedOperationException("");
        } else {
            BlockInfo blockInfo = blockInfoManager.lockForReading(blockId);
            return doGetLocalBytes(blockId, blockInfo);
        }
    }

    private BlockData doGetLocalBytes(BlockId blockId, BlockInfo blockInfo) {
        StorageLevel level = blockInfo.storageLevel;
        if (level.deserialized) {
            if (level.useDisk && diskStore.contains(blockId)) {
                return diskStore.getBytes(blockId);
            } else if (level.useMemory && memoryStore.contains(blockId)) {
                return new ByteBufferBlockData(serializerManager.dataSerializeWithExplicitClassTag(
                        blockId, memoryStore.getValues(blockId)), true);
            } else {
                handleLocalReadFailure(blockId);
            }
        } else {
            if (level.useDisk && diskStore.contains(blockId)) {
                BlockData diskData = diskStore.getBytes(blockId);
                ChunkedByteBuffer cacheBuf = maybeCacheDiskBytesInMemory(blockId, blockInfo, level, diskData);
                if (cacheBuf != null) {
                    return new ByteBufferBlockData(cacheBuf, false);
                }
            } else if (level.useMemory && memoryStore.contains(blockId)) {
                return new ByteBufferBlockData(memoryStore.getBytes(blockId), false);
            } else {
                handleLocalReadFailure(blockId);
            }
        }
        throw new SparkException("");
    }

    /**磁盘读取Block数据块缓存在内存中*/
    private ChunkedByteBuffer maybeCacheDiskBytesInMemory(BlockId blockId, final BlockInfo blockInfo,
                                                          StorageLevel level, BlockData diskData) {
        assert !level.deserialized;
        if (level.useMemory) {
            // 防止多个线程同时将Block缓存内存, 单进程处理
            synchronized (blockInfo) {
                if (memoryStore.contains(blockId)) {
                    // 已有其他线程将Block数据缓存在内存中, 则DiskData释放内存
                    diskData.dispose();
                    return memoryStore.getBytes(blockId);
                }
                // 申请内存空间
                Allocator allocator;
                switch (level.memoryMode()) {
                    case ON_HEAP:
                        allocator = ByteBuffer::allocate;
                        break;
                    case OFF_HEAP:
                        allocator = Platform::allocateDirectBuffer;
                        break;
                    default:
                        throw new SparkException("Unsupported memory mode " + level.memoryMode());
                }
                boolean putSucceeded = memoryStore.putBytes(blockId, blockInfo.size(), level.memoryMode(),
                                                            size -> diskData.toChunkedByteBuffer(allocator));
                if (putSucceeded) {
                    diskData.dispose();
                    return memoryStore.getBytes(blockId);
                }
            }
        }
        return null;
    }

    private void handleLocalReadFailure(BlockId blockId) {
        releaseLock(blockId, -1);
        removeBlock(blockId);
        throw new SparkException("Block " + blockId + " was not found even though it's read-locked");
    }


    public boolean putBytes(BlockId blockId, ChunkedByteBuffer bytes, StorageLevel level) {
        return putBytes(blockId, bytes, level, true);
    }

    public boolean putBytes(BlockId blockId, ChunkedByteBuffer bytes,
                            StorageLevel level, boolean tellMaster) {
        assert bytes != null : "Bytes is null";
        return doPutBytes(blockId, bytes, level, tellMaster, true);
    }

    public void stop() {
        throw new UnsupportedOperationException("");
    }

    private boolean doPutBytes(BlockId blockId, ChunkedByteBuffer bytes, StorageLevel level,
                               boolean tellMaster, boolean keepReadLock) {
        return doPut(blockId, level, tellMaster, keepReadLock, blockInfo -> {
            long startTimeMs = System.currentTimeMillis();
            Future<?> replicationFuture = null;
            if (level.replication > 1) {
                replicationFuture = futureExecutionContext.submit(() -> {
                    BlockData blockData = new ByteBufferBlockData(bytes, false);
                    replicate(blockId, blockData, level);
                });
            }

            if (level.useMemory) {
                boolean putSucceeded = false;
                // Put it in memory first, even if it also has useDisk set to true;
                // We will drop it to disk later if the memory store can't hold it.
                if (level.deserialized) {
                    Iterator<?> values = serializerManager.dataDeserializeStream(blockId, bytes.toInputStream());
                    Pair<MemoryStore.PartiallyUnrolledIterator<?>, Long> result = memoryStore.putIteratorAsValues(blockId, values);
                    if (result.getLeft() != null) {
                        result.getLeft().close();
                        putSucceeded = false;
                    } else {
                        putSucceeded = true;
                    }
                } else {
                    MemoryMode memoryMode = level.memoryMode();
                    putSucceeded = memoryStore.putBytes(blockId, bytes.size(), memoryMode, size -> {
                        boolean exist = false;
                        for (int i = 0; i < bytes.chunks.length; ++i) {
                           if (!bytes.chunks[i].isDirect()) {
                               exist = true;
                               break;
                           }
                        }
                       if (memoryMode == MemoryMode.OFF_HEAP && exist) {
                           return bytes.copy(Platform::allocateDirectBuffer);
                       } else {
                           return bytes;
                       }
                    });
                }

                if (!putSucceeded && level.useDisk) {
                    LOGGER.warn("Persisting block {} to disk instead.", blockId);
                    diskStore.putBytes(blockId, bytes);
                }
            } else if (level.useDisk) {
                diskStore.putBytes(blockId, bytes);
            }

            BlockStatus putBlockStatus = getCurrentBlockStatus(blockId, blockInfo);
            boolean blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid();
            if (blockWasSuccessfullyStored) {
                blockInfo.size = bytes.size();
                if (tellMaster && blockInfo.tellMaster) {
                    reportBlockStatus(blockId, putBlockStatus, 0);
                }
                // TODO: Block Metric
            }
            LOGGER.debug("Put block {} locally took {} ms", blockId, System.currentTimeMillis() - startTimeMs);
            if (level.replication > 1 && replicationFuture != null) {
                // 等待副本创建完成
                try {
                    while (replicationFuture.isDone()) {
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    throw new SparkException("Error occurred while waiting for replication to finish", e);
                }
            }

            if (blockWasSuccessfullyStored) {
                return null;
            }

            return bytes;
        }) == null;
    }

    /**Block数据块创建副本*/
    private void replicate(BlockId blockId, BlockData blockData, StorageLevel level) {
        replicate(blockId, blockData, level, Collections.emptySet());
    }

    private void replicate(BlockId blockId, BlockData blockData,
                                       StorageLevel level, Set<BlockManagerId> existingReplicas) {
        int maxReplicationFailures = conf.getInt("spark.storage.maxReplicationFailures", 1);
        StorageLevel tLevel = StorageLevel.apply(
                level.useDisk,
                level.useMemory,
                level.useOffHeap,
                level.deserialized,
                1
        );

        // 需创建副本数
        int numPeersToReplicateTo = level.replication - 1;
        // Block副本存储地址
        Set<BlockManagerId> peersReplicatedTo = Sets.newHashSet(existingReplicas);
        Set<BlockManagerId> peersFailedToReplicateTo = Sets.newHashSet();
        int numFailures = 0;

        // Block副本存储地址
        Set<BlockManagerId> initialPeers = getPeers(false).stream()
                                                          .filter(existingReplicas::contains)
                                                          .collect(Collectors.toSet());

        // 根据副本地址选择策略, 选择可存储副本的BlockManager
        List<BlockManagerId> peersForReplication = blockReplicationPolicy.prioritize(
                blockInfoManager,
                initialPeers,
                blockId,
                numPeersToReplicateTo
        );

        Iterator<BlockManagerId> iterator = peersForReplication.iterator();
        while (iterator.hasNext() && numFailures <= maxReplicationFailures &&
                peersReplicatedTo.size() < numPeersToReplicateTo) {
            BlockManagerId peer = iterator.next();
            try {
                long onePeerStartTime = System.currentTimeMillis();
                LOGGER.trace("Trying to replicate {} of {} bytes to {}", blockId, blockData.size(), peer);
                blockTransferService.uploadBlock(
                        peer.host,
                        peer.port,
                        peer.executorId,
                        blockId,
                        new BlockManagerManagedBuffer(blockInfoManager, blockId, blockData, false),
                        tLevel
                );
                peersReplicatedTo.add(peer);

                LOGGER.trace("Replicated {} of {} bytes to {} in {} ms",
                             blockId, blockData.size(), peer, System.currentTimeMillis() - onePeerStartTime);
            } catch (Exception e) {
                LOGGER.warn("Failed to replicate {} to {}, failure {}", blockId, peer, numFailures, e);
                peersFailedToReplicateTo.add(peer);

                // 重新选择可缓存的副本的BlockManger地址
                Set<BlockManagerId> filterPeers = getPeers(true).stream().filter(p ->
                        !peersFailedToReplicateTo.contains(p) && !peersReplicatedTo.contains(p)
                ).collect(Collectors.toSet());

                numFailures++;
                peersForReplication = blockReplicationPolicy.prioritize(
                        blockInfoManager,
                        filterPeers,
                        blockId,
                        numPeersToReplicateTo - peersReplicatedTo.size()
                );
                iterator = peersForReplication.iterator();
            }
        }
    }

    private <T> T doPut(BlockId blockId, StorageLevel level, boolean tellMaster,
                        boolean keepReadLock, BlockDataConvert<T> convert) {
        assert blockId != null : "BlockId is null";
        assert level != null && level.isValid() : "StorageLevel is null or invalid";

        BlockInfo blockInfo = new BlockInfo(level, tellMaster);
        if (!blockInfoManager.lockNewBlockForWriting(blockId, blockInfo)) {
            LOGGER.warn("Block {} already exists on this machine; not re-adding it", blockId);
            if (!keepReadLock) {
                releaseLock(blockId, -1);
            }
            return null;
        }

        long startTimeMs = System.currentTimeMillis();
        boolean exceptionWasThrown = true;
        try {
            T result = convert.putBody(blockInfo);
            exceptionWasThrown = false;
            if (result == null) {
                // 数据块存储成功
                if (keepReadLock) {
                    blockInfoManager.downgradeLock(blockId);
                } else {
                    blockInfoManager.unlock(blockId, -1);
                }
            } else {
                removeBlockInternal(blockId, false);
                LOGGER.info("Putting block {} failed", blockId);
            }
            return result;
        } finally {
            if (exceptionWasThrown) {
                LOGGER.warn("Putting block {} failed due to an exception", blockId);
                // If an exception was thrown then it's possible that the code in `putBody` has already
                // notified the master about the availability of this block, so we need to send an update
                // to remove this block location.
                removeBlockInternal(blockId, tellMaster);
                // TODO: Block Metric
            }
        }

    }

    private void removeBlock(BlockId blockId) {
        removeBlock(blockId, true);
    }

    private void removeBlock(BlockId blockId, boolean tellMaster) {
        LOGGER.debug("remove blockId {}", blockId);
        BlockInfo blockInfo = blockInfoManager.lockForWriting(blockId);
        if (blockInfo != null) {
            LOGGER.warn("Asked to remove block {}, which does not exist", blockId);
            return;
        }
        removeBlockInternal(blockId, tellMaster);
        // TODO: Block Metric
    }

    private void removeBlockInternal(BlockId blockId, boolean tellMaster) {
        boolean removedFromMemory = memoryStore.remove(blockId);
        boolean removedFromDisk = diskStore.remove(blockId);

        if (!removedFromDisk && !removedFromMemory) {
            LOGGER.warn("Block {} could not be removed as it was not found on disk or in memory", blockId);
        }
        blockInfoManager.removeBlock(blockId);
        if (tellMaster) {
            reportBlockStatus(blockId, BlockStatus.empty(), 0);
        }
    }

    private Set<BlockManagerId> getPeers(boolean forceFetch) {
        synchronized (peerFetchLock) {
            // 缓存失效时间(ms)
            long cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000);
            boolean timeout = System.currentTimeMillis() - lastPeerFetchTime > cachedPeersTtl;
            if (cachedPeers == null || forceFetch || timeout) {
                cachedPeers = master.getPeers(blockManagerId);
                lastPeerFetchTime = System.currentTimeMillis();
                LOGGER.debug("Fetched peers from master: {}", cachedPeers);
            }
            return cachedPeers;
        }
    }

    interface BlockDataConvert<T> {
        T putBody(BlockInfo blockInfo);
    }

    public interface RDDIterator<T> {
        Iterator<T> makeIterator();
    }
}
