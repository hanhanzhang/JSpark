package com.sdu.spark.shuffle;

import com.google.common.collect.Multimap;
import com.sdu.spark.TaskContext;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.shuffle.ShuffleClient;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.storage.BlockManagerId;
import com.sdu.spark.utils.scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Shuffle Block数据读取
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
    private long maxBytesInFlight;
    private long maxReqsInFlight;
    private int maxBlocksInFlightPerAddress;
    private long maxReqSizeShuffleToMem;
    private boolean detectCorrupt;

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
    }

    private void initialize() {
        context.addTaskCompletionListener(taskContext -> cleanUp());

        //
    }

    @Override
    public File createTempShuffleFile() {
        return null;
    }

    @Override
    public boolean registerTempShuffleFileToClean(File file) {
        return false;
    }

    private void cleanUp() {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tuple2<BlockId, InputStream> next() {
        return null;
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
