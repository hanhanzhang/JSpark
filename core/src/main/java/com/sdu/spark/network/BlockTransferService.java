package com.sdu.spark.network;

import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.buffer.NioManagerBuffer;
import com.sdu.spark.network.shuffle.BlockFetchingListener;
import com.sdu.spark.network.shuffle.ShuffleClient;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.StorageLevel;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author hanhan.zhang
 * */
public abstract class BlockTransferService implements ShuffleClient {

    /**
     * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
     * local blocks or put local blocks.
     */
    public abstract void init(BlockDataManager blockDataManager);

    public abstract void close();
    /**
     * Port number the service is listening on, available only after [[init]] is invoked.
     */
    public abstract int port();

    /**
     * Host name the service is listening on, available only after [[init]] is invoked.
     */
    public abstract String hostName();


    /**
     * Upload a single block to a remote node, available only after [[init]] is invoked.
     */
    public abstract Future<Boolean> uploadBlock(String hostname, int port, String execId, BlockId blockId,
                                             ManagedBuffer blockData, StorageLevel level);


    public ManagedBuffer fetchBlockSync(String host, int port, String execId, String blockId) {
        AtomicReference<ManagedBuffer> buf = new AtomicReference<>();
        fetchBlocks(host, port, execId, ArrayUtils.toArray(blockId), new BlockFetchingListener() {
            @Override
            public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
                try {
                    ByteBuffer ret = ByteBuffer.allocate((int) data.size());
                    ret.put(data.nioByteBuffer());
                    ret.flip();
                    buf.set(new NioManagerBuffer(ret));
                } catch (IOException e) {
                    // ignore
                }
            }

            @Override
            public void onBlockFetchFailure(String blockId, Throwable exception) {

            }
        }, null);
        return buf.get();
    }

}
