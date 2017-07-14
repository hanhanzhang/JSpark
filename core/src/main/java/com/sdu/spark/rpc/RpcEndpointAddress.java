package com.sdu.spark.rpc;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * RpcEndPoint
 *
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class RpcEndpointAddress implements Serializable {
    public String name;
    public RpcAddress address;

    public RpcEndpointAddress(String host, int port, String name) {
        this(name, new RpcAddress(host, port));
    }

    @Override
    public String toString() {
        if (address != null) {
            return String.format("spark://%s@${%s}:${%s}", name, address.host, address.port);
        }
        return String.format("spark-client://%s", name);
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
}
