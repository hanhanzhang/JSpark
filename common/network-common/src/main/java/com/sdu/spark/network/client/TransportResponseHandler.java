package com.sdu.spark.network.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.sdu.spark.network.protocol.*;
import com.sdu.spark.network.server.MessageHandler;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static com.sdu.spark.network.utils.NettyUtils.getRemoteAddress;

/**
 *
 * @author hanhan.zhang
 * */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportResponseHandler.class);

    private final Channel channel;

    private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

    /**
     * Rpc请求响应回调函数[key = requestId, value = callback]
     * */
    private Map<Long, RpcResponseCallback> outstandingRpcCalls;

    private final Queue<Pair<String, StreamCallback>> streamCallbacks;
    private volatile boolean streamActive;

    /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
    private final AtomicLong timeOfLastRequestNs;

    public TransportResponseHandler(Channel channel) {
        this.channel = channel;
        this.outstandingRpcCalls = Maps.newConcurrentMap();
        this.outstandingFetches = Maps.newConcurrentMap();
        this.streamCallbacks = Queues.newLinkedBlockingDeque();
        this.timeOfLastRequestNs = new AtomicLong(0);
    }

    public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
        updateTimeOfLastRequest();
        outstandingFetches.put(streamChunkId, callback);
    }

    public void removeFetchRequest(StreamChunkId streamChunkId) {
        outstandingFetches.remove(streamChunkId);
    }

    public void addRpcRequest(long requestId, RpcResponseCallback callback) {
        updateTimeOfLastRequest();
        outstandingRpcCalls.put(requestId, callback);
    }

    public void removeRpcRequest(long requestId) {
        outstandingRpcCalls.remove(requestId);
    }

    public void addStreamCallback(String streamId, StreamCallback callback) {
        timeOfLastRequestNs.set(System.nanoTime());
        streamCallbacks.offer(ImmutablePair.of(streamId, callback));
    }

    @VisibleForTesting
    public void deactivateStream() {
        streamActive = false;
    }

    @Override
    public void handle(ResponseMessage message) throws Exception {
        if (message instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                LOGGER.warn("Ignoring response for block {} from {} since it is not outstanding",
                        resp.streamChunkId, getRemoteAddress(channel));
                resp.body().release();
            } else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
                resp.body().release();
            }
        } else if (message instanceof ChunkFetchFailure) {
            ChunkFetchFailure resp = (ChunkFetchFailure) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                LOGGER.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
                        resp.streamChunkId, getRemoteAddress(channel), resp.errorString);
            } else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
                        "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
            }
        } else if (message instanceof RpcResponse) {
            RpcResponse resp = (RpcResponse) message;
            RpcResponseCallback listener = outstandingRpcCalls.get(resp.requestId);
            if (listener != null) {
                outstandingRpcCalls.remove(resp.requestId);
                try {
                    listener.onSuccess(message.body().nioByteBuffer());
                } finally {
                    message.body().release();
                }
            } else {
                LOGGER.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel), resp.body().size());
            }
        } else if (message instanceof RpcFailure) {
            RpcFailure resp = (RpcFailure) message;
            RpcResponseCallback listener = outstandingRpcCalls.get(resp.requestId);
            if (listener == null) {
                LOGGER.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel), resp.errorString);
            } else {
                outstandingRpcCalls.remove(resp.requestId);
                listener.onFailure(new RuntimeException(resp.errorString));
            }
        } else if (message instanceof StreamResponse) {
            // TODO: 17/9/23  
        } else if (message instanceof StreamFailure) {
            // TODO: 17/9/23  
        }
    }

    @Override
    public void channelActive() {

    }

    @Override
    public void exceptionCaught(Throwable cause) {

    }

    @Override
    public void channelInActive() {

    }

    public void updateTimeOfLastRequest() {
        timeOfLastRequestNs.set(System.nanoTime());
    }

    public long getTimeOfLastRequestNs() {
        return timeOfLastRequestNs.get();
    }

    public int numOutstandingRequests() {
        return outstandingFetches.size() + outstandingRpcCalls.size() + streamCallbacks.size() +
                (streamActive ? 1 : 0);
    }
}
