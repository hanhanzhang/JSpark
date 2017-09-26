package com.sdu.spark.network.shuffle;

import com.sdu.spark.network.buffer.ManagedBuffer;

import java.util.EventListener;

/**
 * @author hanhan.zhang
 * */
public interface BlockFetchingListener extends EventListener {

    /**
     * Called once per successfully fetched block. After this call returns, data will be released
     * automatically. If the data will be passed to another thread, the receiver should retain()
     * and release() the buffer on their own, or copy the data to a new buffer.
     */
    void onBlockFetchSuccess(String blockId, ManagedBuffer data);

    /**
     * Called at least once per block upon failures.
     */
    void onBlockFetchFailure(String blockId, Throwable exception);

}
