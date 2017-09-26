package com.sdu.spark.network.shuffle;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.utils.NettyUtils;
import com.sdu.spark.network.utils.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 每次请求都会创建{@link RetryingBlockFetcher}, 故线程安全
 *
 * @author hanhan.zhang
 * */
public class RetryingBlockFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryingBlockFetcher.class);

    private BlockFetchStarter fetchStarter;
    private String[] blockIds;
    private BlockFetchingListener listener;

    /**
     * Set of all block ids which have not been fetched successfully or with a non-IO Exception.
     * A retry involves requesting every outstanding block. Note that since this is a LinkedHashSet,
     * input ordering is preserved, so we always request blocks in the same order the user provided.
     */
    private final LinkedHashSet<String> outstandingBlocksIds;

    /** Max number of times we are allowed to retry. */
    private final int maxRetries;
    /** Milliseconds to wait before each retry. */
    private final int retryWaitTime;
    /** Number of times we've attempted to retry so far. */
    private int retryCount = 0;
    private RetryingBlockFetchListener currentListener;

    private static final ExecutorService executorService = Executors.newCachedThreadPool(NettyUtils.createThreadFactory("Block Fetch Retry"));

    public RetryingBlockFetcher(TransportConf conf,
                                BlockFetchStarter fetchStarter,
                                String[] blockIds,
                                BlockFetchingListener listener) {
        this.fetchStarter = fetchStarter;
        this.blockIds =blockIds;
        this.listener = listener;
        this.maxRetries = conf.maxIORetries();
        this.retryWaitTime = conf.ioRetryWaitTimeMs();
        this.outstandingBlocksIds = Sets.newLinkedHashSet();
        Collections.addAll(outstandingBlocksIds, blockIds);
        this.currentListener = new RetryingBlockFetchListener();
    }

    public void start() {
        fetchAllOutstanding();
    }

    private void fetchAllOutstanding() {
        String[] blockIdsToFetch;
        int numRetries;
        RetryingBlockFetchListener fetchListener;
        synchronized (RetryingBlockFetchListener.class) {
            blockIdsToFetch = outstandingBlocksIds.toArray(new String[outstandingBlocksIds.size()]);
            numRetries = retryCount;
            fetchListener = currentListener;
        }

        // 请求数据
        try {
            fetchStarter.createAndStart(blockIdsToFetch, fetchListener);
        } catch (Exception e) {
            LOGGER.error(String.format("Exception while beginning fetch of %s outstanding blocks %s",
                    blockIdsToFetch.length, numRetries > 0 ? "(after " + numRetries + " retries)" : ""), e);
            if (shouldRetry(e)) {
                initiateRetry();
            } else {
                for (String bid : blockIdsToFetch) {
                    listener.onBlockFetchFailure(bid, e);
                }
            }
        }
    }

    private synchronized void initiateRetry() {
        retryCount++;
        currentListener = new RetryingBlockFetchListener();

        LOGGER.info("Retrying fetch ({}/{}) for {} outstanding blocks after {} ms",
                retryCount, maxRetries, outstandingBlocksIds.size(), retryWaitTime);

        executorService.submit(() -> {
            Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
            fetchAllOutstanding();
        });
    }

    private boolean shouldRetry(Throwable t) {
        boolean isIOException = t instanceof IOException ||
                (t.getCause() != null && t.getCause() instanceof IOException);
        boolean hasRemainingRetries = retryCount < maxRetries;
        return isIOException && hasRemainingRetries;
    }

    public interface BlockFetchStarter {
        void createAndStart(String[] blockIds, BlockFetchingListener listener)
                throws IOException, InterruptedException;
    }

    private class RetryingBlockFetchListener implements BlockFetchingListener {

        @Override
        public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
            boolean shouldForwardSuccess = false;
            synchronized (RetryingBlockFetcher.class) {
                if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
                    outstandingBlocksIds.remove(blockId);
                    shouldForwardSuccess = true;
                }
            }
            if (shouldForwardSuccess) {
                listener.onBlockFetchSuccess(blockId, data);
            }
        }

        @Override
        public void onBlockFetchFailure(String blockId, Throwable exception) {
            boolean shouldForwardFailure = false;
            synchronized (RetryingBlockFetcher.this) {
                if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
                    if (shouldRetry(exception)) {
                        initiateRetry();
                    } else {
                        LOGGER.error(String.format("Failed to fetch block %s, and will not retry (%s retries)",
                                blockId, retryCount), exception);
                        outstandingBlocksIds.remove(blockId);
                        shouldForwardFailure = true;
                    }
                }
            }

            // Now actually invoke the parent listener, outside of the synchronized block.
            if (shouldForwardFailure) {
                listener.onBlockFetchFailure(blockId, exception);
            }
        }
    }
}
