package com.sdu.spark.network.netty;

import com.google.common.collect.Lists;
import com.sdu.spark.network.BlockDataManager;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.buffer.NioManagerBuffer;
import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.server.OneForOneStreamManager;
import com.sdu.spark.network.server.RpcHandler;
import com.sdu.spark.network.server.StreamManager;
import com.sdu.spark.network.shuffle.protocol.BlockTransferMessage;
import com.sdu.spark.network.shuffle.protocol.OpenBlocks;
import com.sdu.spark.network.shuffle.protocol.StreamHandle;
import com.sdu.spark.network.shuffle.protocol.UploadBlock;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class NettyBlockRpcServer extends RpcHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyBlockRpcServer.class);

    public String appId;
    public Serializer serializer;
    public BlockDataManager blockManager;

    private OneForOneStreamManager streamManager = new OneForOneStreamManager();

    public NettyBlockRpcServer(String appId, Serializer serializer, BlockDataManager blockManager) {
        this.appId = appId;
        this.serializer = serializer;
        this.blockManager = blockManager;
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        BlockTransferMessage msg = BlockTransferMessage.Decoder.fromByteBuffer(message);
        if (msg instanceof OpenBlocks) {
            OpenBlocks openBlocks = (OpenBlocks) msg;
            List<ManagedBuffer> blockData = Lists.newLinkedList();
            for (int i = 0; i < openBlocks.blockIds.length; ++i) {
                BlockId blockId = BlockId.apply(openBlocks.blockIds[i]);
                blockData.add(blockManager.getBlockData(blockId));
            }
            long streamId = streamManager.registerStream(appId, blockData.iterator());
            LOGGER.info("Registered streamId {} with {} buffers", streamId, openBlocks.blockIds.length);
            callback.onSuccess(new StreamHandle(streamId, openBlocks.blockIds.length).toByteBuffer());
        } else if (msg instanceof UploadBlock) {
            try {
                UploadBlock uploadBlock = (UploadBlock) msg;
                StorageLevel level = serializer.newInstance().deserialize(ByteBuffer.wrap(uploadBlock.metadata));
                BlockId blockId = BlockId.apply(uploadBlock.blockId);
                NioManagerBuffer buf = new NioManagerBuffer(ByteBuffer.wrap(uploadBlock.blockData));
                blockManager.putBlockData(blockId, buf, level);
                callback.onSuccess(ByteBuffer.allocate(0));
            } catch (IOException e) {
                callback.onFailure(e);
            }
        }
    }

    @Override
    public StreamManager getStreamManager() {
        return streamManager;
    }

    @Override
    public void channelActive(TransportClient client) {

    }

    @Override
    public void channelInactive(TransportClient client) {

    }

    @Override
    public void exceptionCaught(Throwable cause, TransportClient client) {

    }
}
