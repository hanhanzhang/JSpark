package com.sdu.spark.network.shuffle;

/**
 * Provides an interface for reading shuffle files, either from an Executor or external service.
 *
 * @author hanhan.zhang
 * */
public interface ShuffleClient {

    /**
     * Initializes the ShuffleClient, specifying this Executor's appId.
     * Must be called before any other method on the ShuffleClient.
     */
    void init(String appId);

    /**
     * Fetch a sequence of blocks from a remote node asynchronously,
     *
     * Note that this API takes a sequence so the implementation can batch requests, and does not
     * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
     * the data of a block is fetched, rather than waiting for all blocks to be fetched.
     *
     * @param host the host of the remote node.
     * @param port the port of the remote node.
     * @param execId the executor id.
     * @param blockIds block ids to fetch.
     * @param listener the listener to receive block fetching status.
     * @param tempShuffleFileManager TempShuffleFileManager to create and clean temp shuffle files.
     *                               If it's not <code>null</code>, the remote blocks will be streamed
     *                               into temp shuffle files to reduce the memory usage, otherwise,
     *                               they will be kept in memory.
     */
    void fetchBlocks(
            String host,
            int port,
            String execId,
            String[] blockIds,
            BlockFetchingListener listener,
            TempShuffleFileManager tempShuffleFileManager);

}
