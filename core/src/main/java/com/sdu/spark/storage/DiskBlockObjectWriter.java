package com.sdu.spark.storage;

import com.sdu.spark.SparkException;
import com.sdu.spark.serializer.SerializationStream;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.serializer.SerializerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;

/**
 * @author hanhan.zhang
 * */
public class DiskBlockObjectWriter extends OutputStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiskBlockObjectWriter.class);

    private File file;
    private SerializerManager serializerManager;
    private SerializerInstance serializerInstance;
    private int bufferSize;
    private boolean syncWrites;
    private BlockId blockId;
    // TODO: Shuffle Metric

    private FileChannel channel;
    private ManualCloseBufferedOutputStream mcs;
    private OutputStream bs;
    private FileOutputStream fos;
    private TimeTrackingOutputStream ts;
    private SerializationStream objOut;
    private boolean initialized = false;
    private boolean streamOpen = false;
    private boolean hasBeenClosed = false;

    private long committedPosition;
    private long reportedPosition;

    private long numRecordsWritten = 0L;


    private class ManualCloseBufferedOutputStream extends BufferedOutputStream {

        ManualCloseBufferedOutputStream(OutputStream out, int size) {
            super(out, size);
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }

    public DiskBlockObjectWriter(File file,
                                 SerializerManager serializerManager,
                                 SerializerInstance serializerInstance,
                                 int bufferSize,
                                 boolean syncWrites) {
        this(file, serializerManager, serializerInstance, bufferSize, syncWrites, null);
    }

    public DiskBlockObjectWriter(File file,
                                 SerializerManager serializerManager,
                                 SerializerInstance serializerInstance,
                                 int bufferSize,
                                 boolean syncWrites,
                                 BlockId blockId) {
        this.file = file;
        this.serializerManager = serializerManager;
        this.serializerInstance = serializerInstance;
        this.bufferSize = bufferSize;
        this.syncWrites = syncWrites;
        this.blockId = blockId;

        this.committedPosition = file.length();
        this.reportedPosition = committedPosition;
    }

    private void initialize() throws IOException {
        fos = new FileOutputStream(file);
        channel = fos.getChannel();
        ts = new TimeTrackingOutputStream(fos);
        mcs = new ManualCloseBufferedOutputStream(ts, bufferSize);
    }

    private DiskBlockObjectWriter open() throws IOException {
        if (hasBeenClosed) {
            throw new IllegalStateException("Writer already closed. Can't be reopened.");
        }
        if (!initialized) {
            initialize();
            initialized = true;
        }
        bs = serializerManager.wrapStream(blockId, mcs);
        objOut = serializerInstance.serializeStream(bs);
        streamOpen = true;
        return this;
    }

    private void closeResources() throws IOException {
        if (initialized) {
            mcs.close();
            channel = null;
            mcs = null;
            bs = null;
            fos = null;
            ts = null;
            objOut = null;
            initialized = false;
            streamOpen = false;
            hasBeenClosed = true;
        }
    }

    public FileSegment commitAndGet() {
        try {
            if (streamOpen) {
                objOut.flush();
                bs.flush();
                objOut.close();
                streamOpen = false;

                if (syncWrites) {
                    long start = System.nanoTime();
                    fos.getFD().sync();
                    // TODO: Shuffle Metric
                }

                long pos = channel.position();
                FileSegment segment = new FileSegment(file, committedPosition, pos - committedPosition);
                committedPosition = pos;
                // TODO: Shuffle Metric
                reportedPosition = committedPosition;
                return segment;
            } else {
                return new FileSegment(file, committedPosition, 0);
            }
        } catch (IOException e) {
            throw new SparkException("flush shuffle block data failure", e);
        }
    }

    public File revertPartialWritesAndClose() {
        try {
            if (initialized) {
                // TODO: Shuffle Metric
                streamOpen = true;
                closeResources();
            }
        } catch (IOException e) {
            throw new SparkException("close failure", e);
        } finally {
            FileOutputStream truncateStream;
            try {
                truncateStream = new FileOutputStream(file, true);
                truncateStream.getChannel().truncate(committedPosition);
                truncateStream.close();
            } catch (Exception e) {
                LOGGER.error("Uncaught exception while reverting partial writes to file {}", file, e);
            }
        }
        return file;
    }

    private void recordWritten() throws IOException {
        numRecordsWritten++;
        // TODO: Shuffle Metric
        if (numRecordsWritten % 16384 == 0) {
            updateBytesWritten();
        }
    }

    private void updateBytesWritten() throws IOException {
        long pos = channel.position();
//        writeMetrics.incBytesWritten(pos - reportedPosition)
        reportedPosition = pos;
    }

    public void write(Object key, Object value) throws IOException {
        if (!streamOpen) {
            open();
        }
        objOut.writeKey(key);
        objOut.writeValue(value);
        recordWritten();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (!streamOpen) {
            open();
        }
        bs.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        objOut.flush();
        bs.flush();
    }

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (initialized) {
            commitAndGet();
            closeResources();
        }
    }
}
