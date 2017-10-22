package com.sdu.spark.shuffle;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.sdu.spark.SparkException;
import com.sdu.spark.TaskContext;
import com.sdu.spark.network.buffer.FileSegmentManagedBuffer;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.shuffle.BlockFetchingListener;
import com.sdu.spark.network.shuffle.OneForOneBlockFetcher;
import com.sdu.spark.network.shuffle.ShuffleClient;
import com.sdu.spark.network.shuffle.TempShuffleFileManager;
import com.sdu.spark.storage.BlockException;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockId.ShuffleBlockId;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.storage.BlockManagerId;
import com.sdu.spark.utils.Utils;
import com.sdu.spark.utils.io.ChunkedByteBufferOutputStream;
import com.sdu.spark.utils.scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static com.sdu.spark.utils.Utils.getUsedTimeMs;

/**
 * {@link ShuffleBlockFetcherIterator}负责Shuffle Block数据读取
 *
 * 1: ShuffleBlockFetcherIterator构造函数会初始化Shuffle Block数据块拉取请求(拆分本进程数据块拉取和跨进程数据块拉取), 对于跨进程
 *
 *    Shuffle Block数据块请求构建{@link FetchRequest}
 *
 * 2: ShuffleBlockFetcherIterator构造函数出启动对Shuffle Block数据块拉取
 *
 *    1': {@link #fetchLocalBlocks()}
 *
 *      拉取本进程Shuffle Block数据({@link BlockManager#getBlockData(BlockId)})
 *
 *    2': {@link #fetchUpToMaxBytes()}
 *
 *      拉取跨进程Shuffle Block数据({@link ShuffleClient#fetchBlocks(String, int, String, String[], BlockFetchingListener, TempShuffleFileManager)})
 *
 * 3: Shuffle Block数据拉取限流
 *
 *    1': {@link #maxBytesInFlight}
 *
 *      'spark.reducer.maxSizeInFlight'控制Shuffle Block最大拉取数据字节数, {@link #bytesInFlight}标识ShuffleBlockFetcherIterator
 *
 *       当前Shuffle Block数据拉取字节数
 *
 *    2': {@link #maxReqsInFlight}
 *
 *      'spark.reducer.maxReqsInFlight'控制Shuffle Block最大拉取数量, {@link #reqsInFlight}标识ShuffleBlockFetcherIterator
 *
 *      当前Shuffle Block数据拉取数量
 *
 *    3': {@link #maxBlocksInFlightPerAddress}
 *
 *      'spark.reducer.maxBlocksInFlightPerAddress'控制对Executor拉取Shuffle Block最大数量, {@link #numBlocksInFlightPerAddress}
 *
 *      维护对每个Executor已进行的Shuffle Block拉取数量
 *
 *  4: Shuffle Block数据传输实现
 *
 *    {@link OneForOneBlockFetcher#start()}请求Shuffle Blocks并将数据陆地磁盘{@link #shuffleFilesSet}
 *
 *  5: Shuffle Block数据拉取结果由{@link #results}记录, {@link SuccessFetchResult#buf}类型为{@link FileSegmentManagedBuffer},
 *
 *    {@link FileSegmentManagedBuffer#file}为Shuffle Block数据落地磁盘文件
 *
 * @author hanhan.zhang
 * */
@SuppressWarnings("ConstantConditions")
public class ShuffleBlockFetcherIterator implements TempShuffleFileManager, Iterator<Tuple2<BlockId, InputStream>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleBlockFetcherIterator.class);

    private TaskContext context;
    private ShuffleClient shuffleClient;
    private BlockManager blockManager;
    private Multimap<BlockManagerId, Tuple2<BlockId, Long>> blocksByAddress;
    private ResultWrapper wrapper;

    /**
     * Shuffle Block请求起始时间
     * */
    private long startTime;

    /**
     * 网络传输Shuffle Block数据最大字节数
     * */
    private long maxBytesInFlight;
    private long maxReqsInFlight;
    /**
     * Executor接收Shuffle Block最大请求数量
     * */
    private int maxBlocksInFlightPerAddress;
    private long maxReqSizeShuffleToMem;
    private boolean detectCorrupt;

    /**Shuffle Block拉取数量, numBlocksToFetch = localBlocks.size + remoteBlocks.size*/
    private int numBlocksToFetch = 0;
    /**本进程(即同Executor)Shuffle Block数据拉取集合*/
    private List<BlockId> localBlocks = Lists.newLinkedList();
    /**跨进程(即不同Executor)Shuffle Block数据拉取集合*/
    private List<BlockId> remoteBlocks = Lists.newLinkedList();
    /**跨进程Shuffle Block数据拉取请求*/
    private Queue<FetchRequest> fetchRequests = new LinkedBlockingQueue<>();
    /**Key = Executor, value = 已发起Shuffle Block拉取数量*/
    private Map<BlockManagerId, Integer> numBlocksInFlightPerAddress = Maps.newHashMap();
    private Map<BlockManagerId, Queue<FetchRequest>> deferredFetchRequests = Maps.newHashMap();

    /**当前Shuffle Block数据拉取结果*/
    private volatile SuccessFetchResult currentResult = null;
    /**当前Shuffle Block数据拉取字节数*/
    private long bytesInFlight = 0L;
    /**当前拉取Shuffle Block数量*/
    private long reqsInFlight = 0;

    /**Shuffle Block拉取数据块遍历用(标识已遍历Shuffle Block数量)*/
    private int numBlocksProcessed = 0;
    /**Shuffle Block*/
    private Set<BlockId> corruptedBlocks = Sets.newHashSet();

    /**Shuffle Block数据拉取后落地磁盘文件集合*/
    private Set<File> shuffleFilesSet = Sets.newHashSet();

    /**Shuffle Block数据拉取结果集合, Shuffle Block遍历集合*/
    private Queue<FetchResult> results = new LinkedBlockingQueue<>();

    /**标识Shuffle Block拉取是否处于激活状态*/
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
                        (reqsInFlight + 1 <= maxReqsInFlight &&
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
        try {
            return blockManager.diskBlockManager.createTempLocalBlock()._2();
        } catch (IOException e) {
            throw new SparkException("create tmp shuffle file failure", e);
        }
    }

    @Override
    public boolean registerTempShuffleFileToClean(File file) {
        if (isZombie) {
            return false;
        } else {
            shuffleFilesSet.add(file);
            return true;
        }
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

    private void throwFetchFailedException(BlockId blockId, BlockManagerId address, Throwable e) {
        if (blockId instanceof ShuffleBlockId) {
            ShuffleBlockId shuffleBlockId = (ShuffleBlockId) blockId;
            throw new FetchFailedException(address,
                                           shuffleBlockId.shuffleId,
                                           shuffleBlockId.mapId,
                                           shuffleBlockId.reduceId,
                                           e);
        } else {
            throw new SparkException("Failed to get block " + blockId + ", which is not a shuffle block", e);
        }
    }

    @Override
    public boolean hasNext() {
        return numBlocksProcessed < numBlocksToFetch;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tuple2<BlockId, InputStream> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        numBlocksProcessed += 1;
        FetchResult result = null;
        InputStream input = null;

        while (result == null) {
            long startFetchWait = System.currentTimeMillis();
            result = results.poll();
            long stopFetchWait = System.currentTimeMillis();
            // TODO: Shuffle Metric
            LOGGER.debug("shuffle fetch wait cost {}ms", stopFetchWait - startFetchWait);

            if (result instanceof SuccessFetchResult) {
                SuccessFetchResult fetchResult = (SuccessFetchResult) result;
                if (!fetchResult.address.equals(blockManager.blockManagerId)) {  //跨进程请求
                    int reqBlocks = numBlocksInFlightPerAddress.get(fetchResult.address);
                    reqBlocks -= 1;
                    numBlocksInFlightPerAddress.put(fetchResult.address, reqBlocks);
                    // TODO: Shuffle Metric
                }
                bytesInFlight -= fetchResult.size;
                if (fetchResult.isNetworkReqDone) {             // BlockManagerId的Shuffle Block全部请求完成
                    reqsInFlight -= 1;
                    LOGGER.debug("Number of requests in flight {}", reqsInFlight);
                }

                InputStream in = null;
                try {
                    in = fetchResult.buf.createInputStream();
                } catch (IOException e) {
                    assert fetchResult.buf instanceof FileSegmentManagedBuffer;
                    LOGGER.error("Failed to create input stream from local block", e);
                    fetchResult.buf.release();
                    throwFetchFailedException(fetchResult.blockId, fetchResult.address, e);
                }

                InputStream inputStream = wrapper.streamWrapper(fetchResult.blockId, in);
                // Only copy the stream if it's wrapped by compression or encryption, also the size of
                // block is small (the decompressed block is smaller than maxBytesInFlight)
                if (detectCorrupt && !inputStream.equals(in) && fetchResult.size < maxBytesInFlight / 3) {
                    ChunkedByteBufferOutputStream out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer::allocate);
                    try {
                        // Decompress the whole block at once to detect any corruption, which could increase
                        // the memory usage tne potential increase the chance of OOM.
                        // TODO: manage the memory used here, and spill it into disk in case of OOM.
                        Utils.copyStream(input, out, false);
                        out.close();
                        input = out.toChunkedByteBuffer().toInputStream(true);
                    } catch (IOException e) {
                        fetchResult.buf.release();
                        if (fetchResult.buf instanceof FileSegmentManagedBuffer ||
                                corruptedBlocks.contains(fetchResult.blockId)) {
                            throwFetchFailedException(fetchResult.blockId, fetchResult.address, e);
                        } else {
                            LOGGER.warn("got an corrupted block {} from {}, fetch again",
                                        fetchResult.blockId, fetchResult.address, e);
                            corruptedBlocks.add(fetchResult.blockId);
                            List<Tuple2<BlockId, Long>> blocks = Lists.newArrayList(new Tuple2<>(fetchResult.blockId, fetchResult.size));
                            fetchRequests.add(new FetchRequest(fetchResult.address, blocks));
                            result = null;
                        }
                    } finally {
                        // TODO: release the buf here to free memory earlier
                        try {
                            inputStream.close();
                            in.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }

            } else if (result instanceof FailureFetchResult) {
                FailureFetchResult failureFetchResult = (FailureFetchResult) result;
                throwFetchFailedException(result.blockId, result.address, failureFetchResult.e);
            }

            fetchUpToMaxBytes();
        }
        currentResult = (SuccessFetchResult) result;
        return new Tuple2<>(currentResult.blockId, new BufferReleasingInputStream(input, this));
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

    private class BufferReleasingInputStream extends InputStream {

        InputStream delegate;
        ShuffleBlockFetcherIterator iterator;

        boolean closed = false;

        BufferReleasingInputStream(InputStream delegate,
                                          ShuffleBlockFetcherIterator iterator) {
            this.delegate = delegate;
            this.iterator = iterator;
        }

        @Override
        public int read() throws IOException {
            return 0;
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }

        @Override
        public synchronized void mark(int readlimit) {
            delegate.mark(readlimit);
        }

        @Override
        public long skip(long n) throws IOException {
            return delegate.skip(n);
        }

        @Override
        public boolean markSupported() {
            return delegate.markSupported();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return delegate.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                delegate.close();
                iterator.releaseCurrentResultBuffer();
                closed = true;
            }
        }

        @Override
        public synchronized void reset() throws IOException {
            delegate.reset();
        }

    }

    public interface ResultWrapper {
        InputStream streamWrapper(BlockId blockId, InputStream inputStream);
    }
}
