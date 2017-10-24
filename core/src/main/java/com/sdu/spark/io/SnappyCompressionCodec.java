package com.sdu.spark.io;

import com.sdu.spark.SparkException;
import com.sdu.spark.rpc.SparkConf;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author hanhan.zhang
 * */
public class SnappyCompressionCodec extends CompressionCodec {

    private SparkConf conf;

    public SnappyCompressionCodec(SparkConf conf) {
        this.conf = conf;
    }

    @Override
    public OutputStream compressedOutputStream(OutputStream s) {
        int blockSize = (int) conf.getSizeAsBytes("spark.io.compression.snappy.blockSize", "32k");
        return new SnappyOutputStreamWrapper(new SnappyOutputStream(s, blockSize));
    }

    @Override
    public InputStream compressedInputStream(InputStream s) {
        try {
            return new SnappyInputStream(s);
        } catch (IOException e) {
            throw new SparkException("create compressed input stream failure", e);
        }
    }


    private final class SnappyOutputStreamWrapper extends OutputStream {

        private SnappyOutputStream sos;
        private boolean closed = false;

        SnappyOutputStreamWrapper(SnappyOutputStream sos) {
            this.sos = sos;
        }

        @Override
        public void write(int b) throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            sos.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            sos.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            sos.flush();
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                sos.close();
            }
        }
    }

}
