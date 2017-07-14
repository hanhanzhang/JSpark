package com.sdu.spark.rpc;

import java.io.Serializable;
import java.net.URI;

/**
 * Rpc节点网络地址
 *
 * @author hanhan.zhang
 * */
public class RpcAddress implements Serializable {

    public String host;

    public int port;

    public RpcAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String hostPort() {
        return host + ":" + port;
    }

    public String toSparkURL() {
        return "Spark://" + hostPort();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        RpcAddress that = (RpcAddress) object;

        if (port != that.port) return false;
        return host != null ? host.equals(that.host) : that.host == null;

    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

    public static RpcAddress fromURI(String rpcURL) {
        try {
            URI uri = new URI(rpcURL);
            String host = uri.getHost();
            int port = uri.getPort();
            return new RpcAddress(host, port);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid rpc url : " + rpcURL);
        }
    }
}
