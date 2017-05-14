package com.sdu.spark.network.sasl;

import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.server.RpcHandler;
import com.sdu.spark.network.server.StreamManager;
import com.sdu.spark.network.utils.JavaUtils;
import com.sdu.spark.network.utils.TransportConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * SASL认证
 *
 * @author hanhan.zhang
 * */
public class SaslRpcHandler extends RpcHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslRpcHandler.class);

    private final TransportConfig conf;

    private final Channel channel;
    /**
     * 消息处理代理类
     * */
    private final RpcHandler delegate;
    /**
     * SASL验证密码获取
     * */
    private final SecretKeyHolder secretKeyHolder;
    /**
     * 是否完成SASl认证
     * */
    private boolean isComplete;

    /**
     * SASL验证中心
     * */
    private SparkSaslServer saslServer;


    public SaslRpcHandler(TransportConfig conf, Channel channel, RpcHandler delegate, SecretKeyHolder secretKeyHolder) {
        this.conf = conf;
        this.channel = channel;
        this.delegate = delegate;
        this.secretKeyHolder = secretKeyHolder;
        this.isComplete = false;
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        if (isComplete) {       // 完成SASL认证
            delegate.receive(client, message, callback);
        }
        if (saslServer == null || !saslServer.isComplete()) {
            ByteBuf nettyBuf = Unpooled.wrappedBuffer(message);
            SaslMessage saslMessage;
            try {
                saslMessage = SaslMessage.decode(nettyBuf);
            } finally {
                nettyBuf.release();
            }

            if (saslServer == null) {
                // First message in the handshake, setup the necessary state.
                client.setClientId(saslMessage.appId);
                saslServer = new SparkSaslServer(saslMessage.appId, secretKeyHolder,
                        conf.saslServerAlwaysEncrypt());
            }

            byte[] response;
            try {
                response = saslServer.response(JavaUtils.bufferToArray(
                        saslMessage.body().nioByteBuffer()));
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            callback.onSuccess(ByteBuffer.wrap(response));
        }

        if (saslServer.isComplete()) {
            if (!SparkSaslServer.QOP_AUTH_CONF.equals(saslServer.getNegotiatedProperty(Sasl.QOP))) {
                LOGGER.debug("SASL authentication successful for channel {}", client);
                complete(true);
                return;
            }

            LOGGER.debug("Enabling encryption for channel {}", client);
//            SaslEncryption.addToChannel(channel, saslServer, conf.maxSaslEncryptedBlockSize());
            complete(false);
        }
    }

    @Override
    public StreamManager getStreamManager() {
        return delegate.getStreamManager();
    }

    @Override
    public void channelActive(TransportClient client) {
        delegate.channelActive(client);
    }

    @Override
    public void channelInactive(TransportClient client) {
        try {
            delegate.channelInactive(client);
        } finally {
            if (saslServer != null) {
                saslServer.dispose();
            }
        }

    }



    @Override
    public void exceptionCaught(Throwable cause, TransportClient client) {
        delegate.exceptionCaught(cause, client);
    }

    private void complete(boolean dispose) {
        if (dispose) {
            try {
                saslServer.dispose();
            } catch (RuntimeException e) {
                LOGGER.error("Error while disposing SASL server", e);
            }
        }

        saslServer = null;
        isComplete = true;
    }
}
