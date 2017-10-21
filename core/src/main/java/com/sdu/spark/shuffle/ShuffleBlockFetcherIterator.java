package com.sdu.spark.shuffle;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.sdu.spark.TaskContext;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.shuffle.BlockFetchingListener;
import com.sdu.spark.network.shuffle.ShuffleClient;
import com.sdu.spark.network.shuffle.TempShuffleFileManager;
import com.sdu.spark.storage.BlockException;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.storage.BlockManagerId;
import com.sdu.spark.utils.Utils;
import com.sdu.spark.utils.scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static com.sdu.spark.utils.Utils.getUsedTimeMs;

/**
 * {@link ShuffleBlockFetcherIterator}负责Shuffle Block数据读取
 *
 * 1: {@link #initialize()} 启动Block数据块请求
 *
 * @author hanhan.zhang
 * */
public class ShuffleBlockFetcherIterator implements TempShuffleFileManager, Iterator<Tuple2<BlockId, InputStream>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleBlockFetcherIterator.class);

    private TaskContext context;
    private ShuffleClient shuffleClient;
    private BlockManager blockManager;
    private Multimap<BlockManagerId, Tuple2<BlockId, Long>> blocksByAddress;
    private ResultWrapper wrapper;

    /**
     * Block请求起始时间
     * */
    private long startTime;

    /**
     * 每次网络传输Block数据最大字节数
     * */
    private long maxBytesInFlight;
    private long maxReqsInFlight;
    /**
     * 每个Executor能接收最大Block数的请求
     * */
    private int maxBlocksInFlightPerAddress;
    private long maxReqSizeShuffleToMem;
    private boolean detectCorrupt;

    /**Block数请求量*/
    private int numBlocksToFetch = 0;
    /**本进程(即同一个Executor)请求的Block集合*/
    private List<BlockId> localBlocks = Lists.newLinkedList();
    /**跨进程(即不同Executor)请求的Block集合*/
    private List<BlockId> remoteBlocks = Lists.newLinkedList();
    /**跨进程Block数据请求*/
    private Queue<FetchRequest> fetchRequests = new LinkedBlockingQueue<>();
    /**每个Executor已请求Block数量*/
    private Map<BlockManagerId, Integer> numBlocksInFlightPerAddress = Maps.newHashMap();
    private Map<BlockManagerId, Queue<FetchRequest>> deferredFetchRequests = Maps.newHashMap();

    /**正在请求中的Block数据*/
    private volatile SuccessFetchResult currentResult = null;
    /**正在请求数据量*/
    private long bytesInFlight = 0L;
    /**正在请求Block数量*/
    private long reqsInFlight = 0;

    /**
     * 存储数据比较大Shuffle Block文件
     * */
    private Set<File> shuffleFilesSet = Sets.newHashSet();

    /**
     * A queue to hold our results. This turns the asynchronous model provided by
     * {@link com.sdu.spark.network.BlockTransferService} into a synchronous model (iterator).
     * */
    private Queue<FetchResult> results = new LinkedBlockingQueue<>();

    /**
     * Whether the iterator is still active. If isZombie is true, the callback interface will no
     * longer place fetched blocks into {@link #results}.
     * */
    private boolean isZombie = false;

    /**
     * @param context [[TaskContext]], used for metrics update
     * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
     * @param blockManager [[BlockManager]] for reading local blocks
     * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
     *                        For each block we also require the size (in bytes as a long field) in
     *                        order to throttle the memory usage.
     * @param wrapper A function to wrap the returned input stream.
     * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
     * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
     * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
     *                                    for a given remote host:port.
     * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
     * @param detectCorrupt whether to detect any corruption in fetched blocks.
     * */
    public ShuffleBlockFetcherIterator(TaskContext context,
                                       ShuffleClient shuffleClient,
                                       BlockManager blockManager,
                                       Multimap<BlockManagerId, Tuple2<BlockId, Long>> blocksByAddress,
                                       ResultWrapper wrapper,
                                       long maxBytesInFlight,
                                       long maxReqsInFlight,
                                       int maxBlocksInFlightPerAddress,
                                       long maxReqSizeShuffleToMem,
                                       boolean detectCorrupt) {
        this.context = context;
        this.shuffleClient = shuffleClient;
        this.blockManager = blockManager;
        this.blocksByAddress = blocksByAddress;
        this.wrapper = wrapper;
        this.maxBytesInFlight = maxBytesInFlight;
        this.maxReqsInFlight = maxReqsInFlight;
        this.maxBlocksInFlightPerAddress = maxBlocksInFlightPerAddress;
        this.maxReqSizeShuffleToMem = maxReqSizeShuffleToMem;
        this.detectCorrupt = detectCorrupt;

        this.startTime = System.currentTimeMillis();
        this.initialize();
    }

    private void initialize() {
        context.addTaskCompletionListener(taskContext -> cleanUp());

        // Block数据块请求拆分本进程和跨进程
        List<FetchRequest> remoteRequests = splitLocalRemoteBlocks();
        Collections.shuffle(remoteRequests);
        fetchRequests.addAll(remoteRequests);

        // 跨进程Block数据块请求
        fetchUpToMaxBytes();
        int numFetches = remoteRequests.size() - fetchRequests.size();
        LOGGER.info("Started {} remote fetches in {}", numFetches, getUsedTimeMs(startTime));

        // 本地进程Block数据请求
        fetchLocalBlocks();
        LOGGER.info("Got local blocks in {}", getUsedTimeMs(startTime));
    }

    private List<FetchRequest> splitLocalRemoteBlocks() {
        long targetRequestSize = Math.max(maxBytesInFlight / 5, 1L);
        LOGGER.debug("maxBytesInFlight: {}, targetRequestSize: {}, maxBlocksInFlightPerAddress: {}",
                     maxBytesInFlight, targetRequestSize, maxBlocksInFlightPerAddress);

        List<FetchRequest> remoteRequests = Lists.newLinkedList();
        int totalBlocks = 0;
        Iterator<BlockManagerId> iter = blocksByAddress.keySet().iterator();
        while (iter.hasNext()) {
            BlockManagerId address = iter.next();
            Collection<Tuple2<BlockId, Long>> blockInfos = blocksByAddress.get(address);
            totalBlocks += blockInfos.size();

            if (address.executorId.equals(blockManager.blockManagerId.executorId)) {    // 同进程请求
                localBlocks.addAll(blockInfos.stream()
                                          .filter(t -> t._2() != 0)
                                          .map(Tuple2::_1).collect(Collectors.toList()));
                numBlocksToFetch += localBlocks.size();
            } else {                                                                    // 跨进程请求
                Iterator<Tuple2<BlockId, Long>> iterator = blockInfos.iterator();
                long curRequestSize = 0L;
                List<Tuple2<BlockId, Long>> curBlocks = Lists.newLinkedList();
                while (iterator.hasNext()) {
                    Tuple2<BlockId, Long> tuple2 = iterator.next();
                    if (tuple2._2() > 0) {
                        curBlocks.add(tuple2);
                        remoteBlocks.add(tuple2._1());
                        numBlocksToFetch += 1;
                        curRequestSize += tuple2._2();
                    } else if (tuple2._2() < 0) {
                        throw new BlockException("Negative block size " + tuple2._2(), tuple2._1());
                    }

                    if (curRequestSize > targetRequestSize || curBlocks.size() >= maxBlocksInFlightPerAddress) {
                        remoteRequests.add(new FetchRequest(address, curBlocks));
                        LOGGER.debug("Creating fetch request of {} at {} with {} blocks",
                                      curRequestSize, address, curBlocks.size());
                        curBlocks = Lists.newLinkedList();
                        curRequestSize = 0;
                    }
                }
                if (curBlocks.size() > 0) {
                    remoteRequests.add(new FetchRequest(address, curBlocks));
                }
            }
        }

        LOGGER.info("Getting {} non-empty blocks out of {} blocks", numBlocksToFetch, totalBlocks);

        return remoteRequests;
    }

    private void sendRequest(FetchRequest request) {
        LOGGER.debug("Sending request for {} blocks ({}) from {}", request.blocks.size(),
                      Utils.bytesToString(request.size), request.address.hostPort());
        bytesInFlight += request.size;
        reqsInFlight += 1;

        Map<String, Long> sizeMap = request.blocks.stream()
                                                  .collect(Collectors.toMap(e -> e._1().toString(), Tuple2::_2));
        Set<String> remainingBlocks = Sets.newHashSet(sizeMap.keySet());
        List<String> blockIds = request.blocks.stream().map(t -> t._1().toString()).collect(Collectors.toList());
        BlockManagerId address = request.address;

        BlockFetchingListener blockFetchingListener = new BlockFetchingListener() {
            @Override
            public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
                synchronized (ShuffleBlockFetcherIterator.this) {
                    if (!isZombie) {
                        // Increment the ref count because we need to pass this to a different thread.
                        // This needs to be released after use.
                        data.retain();
                        remainingBlocks.remove(blockId);
                        results.add(new SuccessFetchResult(BlockId.apply(blockId),
                                                           address,
                                                           sizeMap.get(blockId),
                                                           data,
                                                           remainingBlocks.isEmpty()));
                        LOGGER.debug("remainingBlocks: " + remainingBlocks);
                    }

                    LOGGER.trace("Got remote block {} after ", blockId, Utils.getUsedTimeMs(startTime));
                }
            }

            @Override
            public void onBlockFetchFailure(String blockId, Throwable exception) {
                LOGGER.error("Failed to get block(s) from {}", address.hostPort(), exception);
                results.add(new FailureFetchResult(BlockId.apply(blockId), address, exception));
            }
        };

        if (request.size > maxReqSizeShuffleToMem) {
            shuffleClient.fetchBlocks(address.host,
                                      address.port,
                                      address.executorId,
                                      blockIds.toArray(new String[blockIds.size()]),
                                      blockFetchingListener, this);
        } else {
            shuffleClient.fetchBlocks(address.host,
                                      address.port,
                                      address.executorId,
                                      blockIds.toArray(new String[blockIds.size()]),
                                      blockFetchingListener, null);
        }
    }

    private void send(BlockManagerId remoteAddress, FetchRequest request) {
        sendRequest(request);
        int fetchBlocks = numBlocksInFlightPerAddress.getOrDefault(remoteAddress, 0);
        fetchBlocks += request.blocks.size();
        numBlocksInFlightPerAddress.put(remoteAddress, fetchBlocks);
    }

    private boolean isRemoteBlockFetchable(Queue<FetchRequest> fetchReqQueue) {
        return fetchReqQueue.size() > 0 &&
                (bytesInFlight == 0 ||
                        (bytesInFlight + 1 <= maxReqsInFlight &&
                            bytesInFlight + fetchReqQueue.peek().size <= maxBytesInFlight));
    }

    private boolean isRemoteAddressMaxedOut(BlockManagerId blockManagerId, FetchRequest request) {
        return numBlocksInFlightPerAddress.getOrDefault(blockManagerId, 0) + request.blocks.size() > maxBlocksInFlightPerAddress;
    }

    private void fetchUpToMaxBytes() {
        if (deferredFetchRequests.size() > 0) {
            deferredFetchRequests.forEach((remoteAddress, defQueue) -> {
                // 队列不空且当前请求字节数小于最大请求字节数且Executor未达到最大请求Block数
                while (isRemoteBlockFetchable(defQueue) &&
                        !isRemoteAddressMaxedOut(remoteAddress, defQueue.peek())) {
                    FetchRequest request = defQueue.poll();
                    LOGGER.debug("Processing deferred fetch request for {} with {} blocks",
                                 remoteAddress, request.blocks.size());
                    send(remoteAddress, request);
                }
                if (defQueue.isEmpty()) {
                    deferredFetchRequests.remove(remoteAddress);
                }
            });
        }

        while (isRemoteBlockFetchable(fetchRequests)) {
            FetchRequest request = fetchRequests.poll();
            BlockManagerId address = request.address;
            if (isRemoteAddressMaxedOut(address, request)) {
                LOGGER.debug("Deferring fetch request for {} with {} blocks", address, request.blocks.size());
                Queue<FetchRequest> defReqQueue = deferredFetchRequests.get(address);
                if (defReqQueue == null) {
                    defReqQueue = new LinkedBlockingQueue<>();
                    deferredFetchRequests.put(address, defReqQueue);
                }
                defReqQueue.add(request);
            } else {
                send(address, request);
            }
        }
    }

    /**
     * 本地进程Block数据请求
     * */
    private void fetchLocalBlocks() {
        Iterator<BlockId> iterator = localBlocks.iterator();
        while (iterator.hasNext()) {
            BlockId blockId = iterator.next();
            try {
                ManagedBuffer buf = blockManager.getBlockData(blockId);
                // TODO: Shuffle Metric
                buf.retain();
                results.add(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, false));
            } catch (Exception e) {
                LOGGER.error("Error occurred while fetching local blocks {}", blockId, e);
                results.add(new FailureFetchResult(blockId, blockManager.blockManagerId, e));
            }
        }
    }

    private void releaseCurrentResultBuffer() {
        if (currentResult != null) {
            currentResult.buf.release();
        }
        currentResult = null;
    }

    @Override
    public File createTempShuffleFile() {
        return null;
    }

    @Override
    public boolean registerTempShuffleFileToClean(File file) {
        return false;
    }

    /**
     * 释放所有Shuffle Block数据占用内存
     * */
    private void cleanUp() {
        synchronized (this) {
            isZombie = true;
        }
        releaseCurrentResultBuffer();
        Iterator<FetchResult> iter = results.iterator();
        while (iter.hasNext()) {
            FetchResult result = iter.next();
            if (result instanceof SuccessFetchResult) {
                // TODO: Shuffle Metric
                ((SuccessFetchResult) result).buf.release();
            }
        }

        shuffleFilesSet.forEach(file -> {
            if (!file.delete()) {
                LOGGER.warn("Failed to cleanup shuffle fetch temp file {}", file.getAbsolutePath());
            }
        });

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tuple2<BlockId, InputStream> next() {
        return null;
    }

    private class FetchRequest implements Serializable {
        BlockManagerId address;
        List<Tuple2<BlockId, Long>> blocks;

        long size;

        /**
         * A request to fetch blocks from a remote BlockManager.
         * @param address remote BlockManager to fetch from.
         * @param blocks Sequence of tuple, where the first element is the block id,
         *               and the second element is the estimated size, used to calculate bytesInFlight.
         * */
        public FetchRequest(BlockManagerId address,
                            List<Tuple2<BlockId, Long>> blocks) {
            this.address = address;
            this.blocks = blocks;

            size = blocks.stream().map(Tuple2::_2).reduce((t1, t2) -> t1 + t2).get();
        }
    }

    /**
     * Result of a fetch from a remote block.
     * */
    private class FetchResult implements Serializable {
        BlockId blockId;
        BlockManagerId address;

        FetchResult(BlockId blockId, BlockManagerId address) {
            this.blockId = blockId;
            this.address = address;
        }
    }

    private class SuccessFetchResult extends FetchResult {
        long size;
        ManagedBuffer buf;
        boolean isNetworkReqDone;

        /**
         * @param blockId block id
         * @param address BlockManager that the block was fetched from.
         * @param size estimated size of the block, used to calculate bytesInFlight.
         *             Note that this is NOT the exact bytes.
         * @param buf `ManagedBuffer` for the content.
         * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
         * */
        SuccessFetchResult(BlockId blockId,
                           BlockManagerId address,
                           long size,
                           ManagedBuffer buf,
                           boolean isNetworkReqDone) {
            super(blockId, address);
            assert buf != null;
            assert size > 0;
            this.size = size;
            this.buf = buf;
            this.isNetworkReqDone = isNetworkReqDone;
        }
    }

    private class FailureFetchResult extends FetchResult {
        Throwable e;

        public FailureFetchResult(BlockId blockId,
                                  BlockManagerId address,
                                  Throwable e) {
            super(blockId, address);
            this.e = e;
        }
    }

    public interface ResultWrapper {
        InputStream streamWrapper(BlockId blockId, InputStream inputStream);
    }
}
