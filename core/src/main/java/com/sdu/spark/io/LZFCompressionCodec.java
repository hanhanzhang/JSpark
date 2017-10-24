package com.sdu.spark.io;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;
import com.sdu.spark.SparkException;
import com.sdu.spark.rpc.SparkConf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author hanhan.zhang
 * */
public class LZFCompressionCodec extends CompressionCodec {

    private SparkConf conf;

    public LZFCompressionCodec(SparkConf conf) {
        this.conf = conf;
    }

    @Override
    public OutputStream compressedOutputStream(OutputStream s) {
        return new LZFOutputStream(s).setFinishBlockOnFlush(true);
    }

    @Override
    public InputStream compressedInputStream(InputStream s) {
        try {
            return new LZFInputStream(s);
        } catch (IOException e) {
            throw new SparkException("create compressed input stream failure", e);
        }
    }
}
