package com.sdu.spark.network.server;

import com.google.common.base.Preconditions;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.client.TransportClient;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author hanhan.zhang
 * */
public class OneForOneStreamManager extends StreamManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(OneForOneStreamManager.class);

    private final AtomicLong nextStreamId;

    private final ConcurrentHashMap<Long, StreamState> streams;

    public OneForOneStreamManager() {
        nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
        streams = new ConcurrentHashMap<>();
    }

    private static class StreamState {
        final String appId;
        final Iterator<ManagedBuffer> buffers;

        // The channel associated to the stream
        Channel associatedChannel = null;

        // Used to keep track of the index of the buffer that the user has retrieved, just to ensure
        // that the caller only requests each chunk one at a time, in order.
        int curChunk = 0;

        // Used to keep track of the number of chunks being transferred and not finished yet.
        volatile long chunksBeingTransferred = 0L;

        StreamState(String appId, Iterator<ManagedBuffer> buffers) {
            this.appId = appId;
            this.buffers = Preconditions.checkNotNull(buffers);
        }
    }

    @Override
    public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        StreamState streamState = streams.get(streamId);
        if (streamState.curChunk != chunkIndex) {
            throw new IllegalStateException(String.format(
                    "Received out-of-order chunk index %s (expected %s)", chunkIndex, streamState.curChunk));
        } else if (!streamState.buffers.hasNext()){
            throw new IllegalStateException(String.format(
                    "Requested chunk index beyond end %s", chunkIndex));
        }
        streamState.curChunk += 1;
        ManagedBuffer nextChunk = streamState.buffers.next();

        if (!streamState.buffers.hasNext()) {
            LOGGER.trace("Removing stream id {}", streamId);
            streams.remove(streamId);
        }

        return nextChunk;
    }

    @Override
    public ManagedBuffer openStream(String streamId) {
        Pair<Long, Integer> streamChunkIdPair = parseStreamChunkId(streamId);
        return getChunk(streamChunkIdPair.getLeft(), streamChunkIdPair.getRight());
    }

    @Override
    public void registerChannel(Channel channel, long streamId) {
        if (!streams.containsKey(streamId)) {
            streams.get(streamId).associatedChannel = channel;
        }
    }

    @Override
    public void connectionTerminated(Channel channel) {
        // Close all streams which have been associated with the channel.
        for (Map.Entry<Long, StreamState> entry: streams.entrySet()) {
            StreamState state = entry.getValue();
            if (state.associatedChannel == channel) {
                streams.remove(entry.getKey());

                // Release all remaining buffers.
                while (state.buffers.hasNext()) {
                    state.buffers.next().release();
                }
            }
        }
    }

    @Override
    public long chunksBeingTransferred() {
        // 计算正在发送chunk数
        long sum = 0L;
        for (StreamState streamState: streams.values()) {
            sum += streamState.chunksBeingTransferred;
        }
        return sum;
    }

    @Override
    public void checkAuthorization(TransportClient client, long streamId) {
        StreamState state = streams.get(streamId);
        Preconditions.checkArgument(state != null, "Unknown stream ID.");
        if (client.getClientId() != null) {
            if (!client.getClientId().equals(state.appId)) {
                throw new SecurityException(String.format(
                        "Client %s not authorized to read stream %d (app %s).",
                        client.getClientId(),
                        streamId,
                        state.appId));
            }
        }
    }

    @Override
    public void chunkBeingSent(long streamId) {
        StreamState streamState = streams.get(streamId);
        if (streamState != null) {
            streamState.chunksBeingTransferred++;
        }
    }

    @Override
    public void streamBeingSent(String streamId) {
        chunkBeingSent(parseStreamChunkId(streamId).getLeft());
    }

    @Override
    public void chunkSent(long streamId) {
        StreamState streamState = streams.get(streamId);
        if (streamState != null) {
            streamState.chunksBeingTransferred--;
        }
    }

    @Override
    public void streamSent(String streamId) {
        chunkSent(parseStreamChunkId(streamId).getLeft());
    }

    public static String genStreamChunkId(long streamId, int chunkId) {
        return String.format("%d_%d", streamId, chunkId);
    }

    public static Pair<Long, Integer> parseStreamChunkId(String streamChunkId) {
        String[] array = streamChunkId.split("_");
        assert array.length == 2:
                "Stream id and chunk index should be specified.";
        long streamId = Long.valueOf(array[0]);
        int chunkIndex = Integer.valueOf(array[1]);
        return ImmutablePair.of(streamId, chunkIndex);
    }

    /**
     * Registers a stream of ManagedBuffers which are served as individual chunks one at a time to
     * callers. Each ManagedBuffer will be release()'d after it is transferred on the wire. If a
     * client connection is closed before the iterator is fully drained, then the remaining buffers
     * will all be release()'d.
     *
     * If an app ID is provided, only callers who've authenticated with the given app ID will be
     * allowed to fetch from this stream.
     */
    public long registerStream(String appId, Iterator<ManagedBuffer> buffers) {
        long myStreamId = nextStreamId.getAndIncrement();
        streams.put(myStreamId, new StreamState(appId, buffers));
        return myStreamId;
    }
}


