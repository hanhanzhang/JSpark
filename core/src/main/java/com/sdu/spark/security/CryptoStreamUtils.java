package com.sdu.spark.security;

import com.sdu.spark.rpc.SparkConf;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.channels.WritableByteChannel;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

/**
 * {@link KeyGenerator}生成秘钥
 *
 * @author hanhan.zhang
 * */
public class CryptoStreamUtils {

    /**
     * @return 生成秘钥
     * */
    public static byte[] createKey(SparkConf conf) {
        try {
            int keyLen = conf.getInt("spark.io.encryption.keySizeBits", 128);
            String ioKeyGenAlgorithm = conf.get("spark.io.encryption.keygen.algorithm", "HmacSHA1");
            KeyGenerator keyGen = KeyGenerator.getInstance(ioKeyGenAlgorithm);
            // 秘钥算法对应的keySize一定指定正确, AES算法的keySize是128
            keyGen.init(keyLen);
            return keyGen.generateKey().getEncoded();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

}
