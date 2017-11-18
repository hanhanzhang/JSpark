package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEndpoint;
import com.sdu.spark.rpc.RpcEndpointAddress;
import com.sdu.spark.serializer.SerializationStream;
import com.sdu.spark.utils.ByteBufferInputStream;
import com.sdu.spark.utils.ByteBufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class RequestMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestMessage.class);

    /**
     * 消息发送地址
     * */
    public RpcAddress senderAddress;
    /**
     * 消息接收方
     * */
    public NettyRpcEndpointRef receiver;
    /**
     * 远端服务的客户端
     * */
    public TransportClient client;
    /**
     * 消息体
     * */
    public Object content;

    public RequestMessage(RpcAddress senderAddress, NettyRpcEndpointRef receiver, Object content) {
        this.senderAddress = senderAddress;
        this.receiver = receiver;
        this.content = content;
    }

    /**
     * 序列化:
     *
     *  1: 发送方地址
     *  2: 接收方地址
     *  3: 接收方{@link RpcEndpoint}名字
     *  4: 发送内容
     * */
    public ByteBuffer serialize(NettyRpcEnv rpcEnv) {
        ByteBufferOutputStream bos = new ByteBufferOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            writeRpcAddress(out, senderAddress);
            writeRpcAddress(out, receiver.address());
            out.writeUTF(receiver.name());

            // 发送消息体
            SerializationStream s = rpcEnv.serializeStream(out);
            try {
                s.writeObject(content);
                return bos.toByteBuffer();
            } finally {
                s.close();
            }
        } catch (IOException e) {
            LOGGER.error("serialize request message error", e);
            throw new IllegalStateException("serialize request message error", e);
        } finally {
            close(out);
        }
    }

    /**
     * 反序列化:
     *
     *  1: 发送方地址
     *  2: 接收方地址
     *  3: 接收方{@link RpcEndpoint}名字
     *  4: 发送内容
     * */
    public static RequestMessage deserialize(ByteBuffer buffer, NettyRpcEnv rpcEnv, TransportClient client) {
        ByteBufferInputStream bis = new ByteBufferInputStream(buffer);
        DataInputStream input = new DataInputStream(bis);
        try {
            RpcAddress senderAddress = readRpcAddress(input);
            RpcAddress receiverAddress = readRpcAddress(input);
            String name = input.readUTF();
            // 消息体
            RpcEndpointAddress endpointAddress = new RpcEndpointAddress(name, receiverAddress);
            NettyRpcEndpointRef endPointRef = new NettyRpcEndpointRef(endpointAddress, rpcEnv);
            endPointRef.client = client;

            return new RequestMessage(senderAddress, endPointRef, rpcEnv.deserialize(client, buffer));
        } catch (Exception e) {
            // ignore
            throw new IllegalStateException("deserialize error");
        } finally {
            close(input);
        }
    }

    private static void writeRpcAddress(DataOutputStream out, RpcAddress address) throws IOException {
        if (address == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(address.host);
            out.writeInt(address.port);
        }
    }

    private static RpcAddress readRpcAddress(DataInputStream input) throws IOException {
        boolean isNull = input.readBoolean();
        if (!isNull) {
            return null;
        }
        String host = input.readUTF();
        int port = input.readInt();
        return new RpcAddress(host, port);
    }

    private static void close(OutputStream out) {
        if (out == null) {
            return;
        }
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void close(InputStream is) {
        if (is == null) {
            return;
        }
        try {
            is.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
