package com.sdu.spark.network.buffer;

import com.google.common.io.ByteStreams;
import com.sdu.spark.network.utils.JavaUtils;
import com.sdu.spark.network.utils.LimitedInputStream;
import com.sdu.spark.network.utils.TransportConf;
import io.netty.channel.DefaultFileRegion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * @author hanhan.zhang
 * */
public class FileSegmentManagedBuffer extends ManagedBuffer {

    private final TransportConf conf;
    private final File file;
    private final long offset;
    private final long length;

    public FileSegmentManagedBuffer(TransportConf conf, File file, long offset, long length) {
        this.conf = conf;
        this.file = file;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public Object convertToNetty() throws IOException {
        if (conf.lazyFileDescriptor()) {
            return new DefaultFileRegion(file, offset, length);
        } else {
            FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            return new DefaultFileRegion(fileChannel, offset, length);
        }
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        FileChannel channel = null;
        try {
            channel = new RandomAccessFile(file, "r").getChannel();
            if (conf.memoryMapBytes() > length) {
                ByteBuffer buf = ByteBuffer.allocate((int) length);
                channel.position(offset);
                while (buf.remaining() != 0) {
                    if (channel.read(buf) == -1) {
                        throw new IOException(String.format("Reached EOF before filling buffer\n" +
                                        "offset=%s\nfile=%s\nbuf.remaining=%s",
                                offset, file.getAbsoluteFile(), buf.remaining()));
                    }
                }
                buf.flip();
                return buf;
            } else {
                return channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
            }
        } catch (IOException e) {
            try {
                if (channel != null) {
                    long size = channel.size();
                    throw new IOException("Error in reading " + this + " (actual file length " + size + ")", e);
                }
            } catch (IOException ignored) {
                // ignore
            }
            throw new IOException("Error in opening " + this, e);
        } finally {
            JavaUtils.closeQuietly(channel);
        }
    }

    @Override
    public ManagedBuffer release() {
        return this;
    }

    @Override
    public ManagedBuffer retain() {
        return this;
    }

    @Override
    public InputStream createInputStream() throws IOException {
        InputStream is = null;
        try {
            is = Files.newInputStream(file.toPath());
            ByteStreams.skipFully(is, offset);
            return new LimitedInputStream(is, length);
        } catch (IOException e) {
            try {
                if (is != null) {
                    long size = file.length();
                    throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
                            e);
                }
            } catch (IOException ignored) {
                // ignore
            } finally {
                JavaUtils.closeQuietly(is);
            }
            throw new IOException("Error in opening " + this, e);
        } catch (RuntimeException e) {
            JavaUtils.closeQuietly(is);
            throw e;
        }
    }
}
