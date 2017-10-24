package com.sdu.spark.io;

import com.sdu.spark.rpc.SparkConf;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author hanhan.zhang
 * */
public class LZ4CompressionCodec extends CompressionCodec {

    private SparkConf conf;

    public LZ4CompressionCodec(SparkConf conf) {
        this.conf = conf;
    }

    @Override
    public OutputStream compressedOutputStream(OutputStream s) {
        int blockSize = (int) conf.getSizeAsBytes("spark.io.compression.lz4.blockSize", "32k");
        return new LZ4BlockOutputStream(s, blockSize);
    }

    @Override
    public InputStream compressedInputStream(InputStream s) {
        return new LZ4BlockInputStream(s);
    }
}
