package com.sdu.spark.network.sasl;

import javax.security.sasl.SaslException;

/**
 * @author hanhan.zhang
 * */
public interface SaslEncryptionBackend {
    /** Disposes of resources used by the backend. */
    void dispose();

    /**数据加密*/
    byte[] wrap(byte[] data, int offset, int len) throws SaslException;

    /**数据解密*/
    byte[] unwrap(byte[] data, int offset, int len) throws SaslException;
}
