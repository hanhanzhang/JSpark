package com.sdu.spark.network.sasl;

import javax.security.sasl.SaslException;

/**
 * @author hanhan.zhang
 * */
public interface SaslEncryptionBackend {
    /** Disposes of resources used by the backend. */
    void dispose();

    /** Encrypt data. */
    byte[] wrap(byte[] data, int offset, int len) throws SaslException;

    /** Decrypt data. */
    byte[] unwrap(byte[] data, int offset, int len) throws SaslException;
}
