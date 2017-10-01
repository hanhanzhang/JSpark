package com.sdu.spark.network.shuffle;

import com.google.common.collect.Lists;
import com.sdu.spark.network.TransportContext;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.TransportClientBootstrap;
import com.sdu.spark.network.client.TransportClientFactory;
import com.sdu.spark.network.crypto.AuthClientBootstrap;
import com.sdu.spark.network.sasl.SecretKeyHolder;
import com.sdu.spark.network.server.NoOpRpcHandler;
import com.sdu.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import com.sdu.spark.network.shuffle.protocol.RegisterExecutor;
import com.sdu.spark.network.utils.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Client for reading shuffle blocks which points to an external (outside of executor) server.
 * This is instead of reading shuffle blocks directly from other executors (via
 * BlockTransferService), which has the downside of losing the shuffle data if we lose the
 * executors.
 */
public class ExternalShuffleClient implements ShuffleClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalShuffleClient.class);

    private final TransportConf conf;
    private final boolean authEnabled;
    private final SecretKeyHolder secretKeyHolder;
    private final long registrationTimeoutMs;

    protected TransportClientFactory clientFactory;
    protected String appId;

    public ExternalShuffleClient(
            TransportConf conf,
            SecretKeyHolder secretKeyHolder,
            boolean authEnabled,
            long registrationTimeoutMs) {
        this.conf = conf;
        this.secretKeyHolder = secretKeyHolder;
        this.authEnabled = authEnabled;
        this.registrationTimeoutMs = registrationTimeoutMs;
    }


    @Override
    public void init(String appId) {
        this.appId = appId;
        TransportContext context = new TransportContext(conf, new NoOpRpcHandler());
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        if (authEnabled) {
            bootstraps.add(new AuthClientBootstrap(conf, appId, secretKeyHolder));
        }
        clientFactory = context.createClientFactory();
    }

    @Override
    public void fetchBlocks(String host, int port, String execId, String[] blockIds,
                            BlockFetchingListener listener, TempShuffleFileManager tempShuffleFileManager) {
        checkInit();
        LOGGER.info("External shuffle fetch from {}:{} (executor id {})", host, port, execId);
        try {
            RetryingBlockFetcher.BlockFetchStarter blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter() {
                @Override
                public void createAndStart(String[] blockIds, BlockFetchingListener listener) throws IOException, InterruptedException {
                    TransportClient client = clientFactory.createClient(host, port);
                    new OneForOneBlockFetcher(client, appId, execId, blockIds, listener, conf, tempShuffleFileManager).start();
                }
            };

            int maxRetries = conf.maxIORetries();
            if (maxRetries > 0) {
                // RetryingBlockFetcher.start()
                //          --> BlockFetchStarter.createAndStart()
                //                      --> OneForOneBlockFetcher.start()
                new RetryingBlockFetcher(conf, blockFetchStarter, blockIds, listener).start();
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

    protected void checkInit() {
        assert appId != null : "Called before init()";
    }

    public void registerWithShuffleServer(
            String host,
            int port,
            String execId,
            ExecutorShuffleInfo executorInfo) throws IOException, InterruptedException {
        checkInit();
        try (TransportClient client = clientFactory.createUnmanagedClient(host, port)) {
            ByteBuffer registerMessage = new RegisterExecutor(appId, execId, executorInfo).toByteBuffer();
            client.sendRpcSync(registerMessage, registrationTimeoutMs);
        }
    }
}
