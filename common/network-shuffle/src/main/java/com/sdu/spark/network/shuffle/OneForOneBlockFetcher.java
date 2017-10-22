package com.sdu.spark.network.shuffle;

import com.sdu.spark.network.buffer.FileSegmentManagedBuffer;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.client.ChunkReceivedCallback;
import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.StreamCallback;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.shuffle.protocol.BlockTransferMessage;
import com.sdu.spark.network.shuffle.protocol.OpenBlocks;
import com.sdu.spark.network.shuffle.protocol.StreamHandle;
import com.sdu.spark.network.utils.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.util.Arrays;

/**
 *
 *
 * @author hanhan.zhang
 * */
public class OneForOneBlockFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(OneForOneBlockFetcher.class);

    private final TransportClient client;
    private final OpenBlocks openMessage;
    private final String[] blockIds;
    private final BlockFetchingListener listener;
    private final ChunkReceivedCallback chunkCallback;
    private final TransportConf transportConf;
    private final TempShuffleFileManager tempShuffleFileManager;

    private StreamHandle streamHandle = null;

    public OneForOneBlockFetcher(
            TransportClient client,
            String appId,
            String execId,
            String[] blockIds,
            BlockFetchingListener listener,
            TransportConf transportConf) {
        this(client, appId, execId, blockIds, listener, transportConf, null);
    }

    public OneForOneBlockFetcher(
            TransportClient client,
            String appId,
            String execId,
            String[] blockIds,
            BlockFetchingListener listener,
            TransportConf transportConf,
            TempShuffleFileManager tempShuffleFileManager) {
        this.client = client;
        this.openMessage = new OpenBlocks(appId, execId, blockIds);
        this.blockIds = blockIds;
        this.listener = listener;
        this.chunkCallback = new ChunkCallback();
        this.transportConf = transportConf;
        this.tempShuffleFileManager = tempShuffleFileManager;
    }

    public void start() {
        if (blockIds.length == 0) {
            throw new IllegalArgumentException("Zero-sized blockIds array");
        }
        client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                try {
                    streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
                    LOGGER.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);
                    // 逐个请求
                    for (int i = 0; i < streamHandle.numChunks; ++i) {
                        if (tempShuffleFileManager != null) {
                            client.stream(genStreamChunkId(streamHandle.streamId, i), new DownloadCallback(i));
                        } else {
                            client.fetchChunk(streamHandle.streamId, i, new ChunkCallback());
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed while starting block fetches after success", e);
                    failRemainingBlocks(blockIds, e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                LOGGER.error("Failed while starting block fetches", e);
                failRemainingBlocks(blockIds, e);
            }
        });
    }

    private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
        for (String blockId : failedBlockIds) {
            try {
                listener.onBlockFetchFailure(blockId, e);
            } catch (Exception e2) {
                LOGGER.error("Error in block fetch failure callback", e2);
            }
        }
    }

    private static String genStreamChunkId(long streamId, int chunkId) {
        return String.format("%d_%d", streamId, chunkId);
    }

    private class DownloadCallback implements StreamCallback {

        private WritableByteChannel channel = null;
        private File targetFile = null;
        private int chunkIndex;

        DownloadCallback(int chunkIndex) throws IOException {
            this.targetFile = tempShuffleFileManager.createTempShuffleFile();
            this.channel = Channels.newChannel(Files.newOutputStream(targetFile.toPath()));
            this.chunkIndex = chunkIndex;
        }

        @Override
        public void onData(String streamId, ByteBuffer buf) throws IOException {
            channel.write(buf);
        }

        @Override
        public void onComplete(String streamId) throws IOException {
            channel.close();
            channel.close();
            // 缓解内存使用
            ManagedBuffer buffer = new FileSegmentManagedBuffer(transportConf, targetFile, 0, targetFile.length());
            listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
            if (!tempShuffleFileManager.registerTempShuffleFileToClean(targetFile)) {
                // 使用方不处理Shuffle Block数据文件, 由自身对Shuffle Block文件做删除操作
                targetFile.delete();
            }
        }

        @Override
        public void onFailure(String streamId, Throwable cause) throws IOException {
            channel.close();
            // On receipt of a failure, fail every block from chunkIndex onwards.
            String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
            failRemainingBlocks(remainingBlockIds, cause);
            targetFile.delete();
        }

    }

    private class ChunkCallback implements ChunkReceivedCallback {
        @Override
        public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
            listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
        }

        @Override
        public void onFailure(int chunkIndex, Throwable e) {
            String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
            failRemainingBlocks(remainingBlockIds, e);
        }
    }
}
