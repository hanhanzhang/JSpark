package com.sdu.spark.network.netty;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.network.BlockDataManager;
import com.sdu.spark.network.BlockTransferService;
import com.sdu.spark.network.TransportContext;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.TransportClientBootstrap;
import com.sdu.spark.network.client.TransportClientFactory;
import com.sdu.spark.network.crypto.AuthClientBootstrap;
import com.sdu.spark.network.crypto.AuthServerBootstrap;
import com.sdu.spark.network.server.RpcHandler;
import com.sdu.spark.network.server.TransportServer;
import com.sdu.spark.network.server.TransportServerBootstrap;
import com.sdu.spark.network.shuffle.BlockFetchingListener;
import com.sdu.spark.network.shuffle.OneForOneBlockFetcher;
import com.sdu.spark.network.shuffle.RetryingBlockFetcher;
import com.sdu.spark.network.shuffle.TempShuffleFileManager;
import com.sdu.spark.network.shuffle.protocol.UploadBlock;
import com.sdu.spark.network.utils.JavaUtils;
import com.sdu.spark.network.utils.TransportConf;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.serializer.JavaSerializer;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @author hanhan.zhang
 * */
public class NettyBlockTransferService extends BlockTransferService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyBlockTransferService.class);

    public SparkConf conf;
    public SecurityManager securityManager;
    public String bindAddress;
    public int port;
    public int numCores;

    private Serializer serializer;
    private boolean authEnabled;
    private TransportConf transportConf;

    private TransportContext transportContext;
    private TransportServer server;
    private TransportClientFactory clientFactory;
    private String appId;

    public NettyBlockTransferService(SparkConf conf, SecurityManager securityManager, String bindAddress,
                                     int port, int numCores) {
        this.conf = conf;
        this.securityManager = securityManager;
        this.bindAddress = bindAddress;
        this.port = port;
        this.numCores = numCores;

        this.serializer = new JavaSerializer(this.conf);
        this.authEnabled = this.securityManager.isAuthenticationEnabled();
        this.transportConf = SparkTransportConf.fromSparkConf(this.conf, "shuffle", numCores);
    }

    @Override
    public void init(String appId) {

    }


    @Override
    public void init(BlockDataManager blockDataManager) {
        RpcHandler rpcHandler = new NettyBlockRpcServer(conf.getAppId(), this.serializer, blockDataManager);
        this.transportContext = new TransportContext(transportConf, rpcHandler);
        List<TransportServerBootstrap> serverBootstraps = Lists.newLinkedList();
        List<TransportClientBootstrap> clientBootstraps = Lists.newLinkedList();
        if (authEnabled) {
            serverBootstraps.add(new AuthServerBootstrap(transportConf, securityManager));
            clientBootstraps.add(new AuthClientBootstrap(transportConf, conf.getAppId(), securityManager));
        }

        this.clientFactory = this.transportContext.createClientFactory(clientBootstraps);
        this.server = transportContext.createServer(bindAddress, port, serverBootstraps);

        appId = conf.getAppId();
        LOGGER.info("Block transport server created on {}:{}", bindAddress, port);
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public String hostName() {
        return bindAddress;
    }

    @Override
    public void fetchBlocks(String host, int port, String execId, String[] blockIds, BlockFetchingListener listener, TempShuffleFileManager tempShuffleFileManager) {
        try {
            RetryingBlockFetcher.BlockFetchStarter blockFetchStarter = (fetchBlockIds, blockFetchingListener) -> {
                TransportClient client = clientFactory.createClient(host, port);
                new OneForOneBlockFetcher(client, appId, execId, blockIds, blockFetchingListener,
                                            transportConf, tempShuffleFileManager).start();
            };


            if (transportConf.maxIORetries() > 0) {
                // RetryingBlockFetcher.start()
                //      --> BlockFetchStarter.createAndStart()
                //              --> OneForOneBlockFetcher.start()
                new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start();
            } else {
                blockFetchStarter.createAndStart(blockIds, listener);
            }
        } catch (Exception e) {
            LOGGER.error("Exception while beginning fetchBlocks", e);
            for (String blockId : blockIds) {
                listener.onBlockFetchFailure(blockId, e);
            }
        }
    }

    @Override
    public Future<Boolean> uploadBlock(String hostname, int port, String execId, BlockId blockId, ManagedBuffer blockData, StorageLevel level) {
        try {
            TransportClient client = clientFactory.createClient(hostname, port);
            byte[] metadata = JavaUtils.bufferToArray(serializer.newInstance().serialize((level)));
            byte[] blockDataBytes = JavaUtils.bufferToArray(blockData.nioByteBuffer());

            NettyBlockUploadCallback uploadCallback = new NettyBlockUploadCallback();
            client.sendRpc(new UploadBlock(appId, execId, blockId.toString(), metadata, blockDataBytes).toByteBuffer(), uploadCallback);

            return uploadCallback.getResponseFuture();
        } catch (Exception e) {
            // ignore
            throw new RuntimeException(e);
        }
    }


    @Override
    public void close() {
        try {
            if (server != null) {
                server.close();
            }
            if (clientFactory != null) {
                clientFactory.close();
            }
        } catch (IOException e) {

        }

    }
}
