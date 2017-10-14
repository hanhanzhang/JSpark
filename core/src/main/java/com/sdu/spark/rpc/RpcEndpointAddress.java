package com.sdu.spark.rpc;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * RpcEndPoint
 *
 * @author hanhan.zhang
 * */
public class RpcEndpointAddress implements Serializable {
    public String name;
    public RpcAddress address;

    public RpcEndpointAddress(String name, RpcAddress address) {
        this.name = name;
        this.address = address;
    }

    public RpcEndpointAddress(String host, int port, String name) {
        this(name, new RpcAddress(host, port));
    }

    @Override
    public String toString() {
        if (address != null) {
            return String.format("spark://%s@%s:%s", name, address.host, address.port);
        }
        return String.format("spark-client://%s", name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RpcEndpointAddress that = (RpcEndpointAddress) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return address != null ? address.equals(that.address) : that.address == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (address != null ? address.hashCode() : 0);
        return result;
    }

    public static RpcEndpointAddress apply(String sparkUrl) {
        try {
            URI uri = new URI(sparkUrl);
            String host = uri.getHost();
            int port = uri.getPort();
            String name = uri.getUserInfo();
            if (!uri.getScheme().equals("spark") || host == null || port < 0 || name == null ||
                    StringUtils.isNotEmpty(uri.getPath()) || uri.getFragment() != null ||
                    uri.getQuery() != null) {
                throw new IllegalArgumentException("Invalid Spark URL: " + sparkUrl);
            }
            return new RpcEndpointAddress(host, port, name);
        } catch (URISyntaxException e){
            throw new RuntimeException(e);
        }
    }

    public static RpcEndpointAddress apply(String host, int port, String name) {
        return new RpcEndpointAddress(host, port, name);
    }
}
