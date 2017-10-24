package com.sdu.spark.io;

import com.google.common.collect.Maps;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.utils.Utils;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.Map;

/**
 *
 * @author hanhan.zhang
 * */
public abstract class CompressionCodec {

    private static final Map<String, String> shortCompressionCodecNames;

    private static final String configKey = "spark.io.compression.codec";
    private static final String DEFAULT_COMPRESSION_CODEC = "lz4";
    private static final String FALLBACK_COMPRESSION_CODEC = "snappy";

    static {
        shortCompressionCodecNames = Maps.newHashMap();
        shortCompressionCodecNames.put("lz4", LZ4CompressionCodec.class.getName());
        shortCompressionCodecNames.put("lzf", LZFCompressionCodec.class.getName());
        shortCompressionCodecNames.put("snappy", SnappyCompressionCodec.class.getName());
    }

    private static String getCodecName(SparkConf conf) {
        return conf.get(configKey, DEFAULT_COMPRESSION_CODEC);
    }

    public static CompressionCodec createCodec(SparkConf conf) {
        return createCodec(conf, getCodecName(conf));
    }

    public static CompressionCodec createCodec(SparkConf conf, String codecName) {
        String codecClass = shortCompressionCodecNames.getOrDefault(codecName.toLowerCase(), codecName);
        CompressionCodec codec = null;
        try {
            Constructor<?> ctor = Utils.classForName(codecClass).getConstructor(SparkConf.class);
            codec = (CompressionCodec) ctor.newInstance(conf);
        } catch (Exception e) {
            // ignore
        }
        if (codec == null) {
            throw new IllegalArgumentException(String.format("Codec [%s] is not available. Consider setting %s=%s",
                                                             codecName, configKey, FALLBACK_COMPRESSION_CODEC));
        }
        return codec;
    }

    public abstract OutputStream compressedOutputStream(OutputStream s);

    public abstract InputStream compressedInputStream(InputStream s);

}
