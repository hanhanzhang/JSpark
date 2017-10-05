package com.sdu.spark.rpc.netty;


import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.sdu.spark.SparkException;
import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.rpc.*;
import com.sdu.spark.utils.ThreadUtils;
import com.sdu.spark.rpc.netty.IndexMessage.*;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

/**
 * {@link Dispatcher}负责路由接收到的消息[本地消息及网络消息]给{@link RpcEndPoint}
 *
 * @author hanhan.zhang
 * */
public class Dispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);

    private NettyRpcEnv nettyRpcEnv;

    /*****************************Spark Point-To-Point映射*******************************/
    // key = RpcEndPoint Name, value = EndPointData
    private Map<String, EndPointData> endPoints = Maps.newConcurrentMap();
    // key = RpcEndPoint, value = RpcEndPointRef
    private Map<RpcEndPoint, RpcEndPointRef> endPointRefs = Maps.newConcurrentMap();

    //
    private LinkedBlockingQueue<EndPointData> receivers = new LinkedBlockingQueue<>();


    private Boolean stopped = false;

    // 消息分发工作线程
    private ThreadPoolExecutor pool;

    private static final int DEFAULT_DISPATCHER_THREADS = 5;

    public Dispatcher(NettyRpcEnv nettyRpcEnv, SparkConf conf) {
        this.nettyRpcEnv = nettyRpcEnv;

        int threads = conf.getInt("spark.rpc.netty.dispatcher.numThreads", Runtime.getRuntime().availableProcessors());
        if (threads <= 0) {
            threads = DEFAULT_DISPATCHER_THREADS;
        }

        pool = ThreadUtils.newDaemonCachedThreadPool("dispatcher-event-loop-%d", threads, 60);
        // 启动消息处理任务
        for (int i = 0; i < threads; ++i) {
            pool.execute(new MessageLoop());
        }
    }


    /*********************************Spark Point-To-Point映射管理************************************/
    // RpcEndPoint节点注册, 并返回RpcEndPoint节点的引用
    public NettyRpcEndPointRef registerRpcEndPoint(String name, RpcEndPoint endPoint) {
        RpcEndpointAddress endpointAddress = new RpcEndpointAddress(name, nettyRpcEnv.address());
        NettyRpcEndPointRef endPointRef = new NettyRpcEndPointRef(endpointAddress, nettyRpcEnv);
        synchronized (this) {
            if (stopped) {
                throw new IllegalStateException("RpcEnv has stopped");
            }
            if (endPoints.putIfAbsent(name, new EndPointData(name, endPoint, endPointRef)) != null) {
                throw new IllegalArgumentException("There is already an RpcEndpoint called " + name);
            }
            EndPointData data = endPoints.get(name);
            endPointRefs.put(data.endPoint, data.endPointRef);
            receivers.offer(data);
        }
        return endPointRef;
    }

    public void unregisterRpcEndpoint(String name) {
        EndPointData data = endPoints.remove(name);
        if (data != null) {
            data.index.stop();
            receivers.offer(data);
        }
    }

    public RpcEndPointRef verify(String name) {
        EndPointData endPointData = endPoints.get(name);
        if (endPointData == null) {
            return null;
        }
        return endPointData.endPointRef;
    }

    public RpcEndPointRef getRpcEndPointRef(RpcEndPoint endPoint) {
        return endPointRefs.get(endPoint);
    }

    public void removeRpcEndPointRef(RpcEndPoint endPoint) {
        endPointRefs.remove(endPoint);
    }

    /******************************Spark Message路由*******************************/
    // 本地消息
    public Object postLocalMessage(RequestMessage req) throws ExecutionException, InterruptedException {
//        NettyLocalResponseCallback<Object> callback = new NettyLocalResponseCallback<>();
        SettableFuture<Object> p = SettableFuture.create();
        LocalNettyRpcCallContext callContext = new LocalNettyRpcCallContext(req.senderAddress, p);
        RpcMessage rpcMessage = new RpcMessage(req.senderAddress, req.content, callContext);
        postMessage(req.receiver.name(), rpcMessage, null);

        return p.get();
    }
    // 网络消息[单向]
    public void postOneWayMessage(RequestMessage req) {
        OneWayMessage oneWayMessage = new OneWayMessage(req.senderAddress, req.content);
        postMessage(req.receiver.name(), oneWayMessage, e -> {
            if (e instanceof RpcEnvStoppedException) {
                throw (RpcEnvStoppedException) e;
            } else if (e instanceof SparkException) {
                throw (SparkException) e;
            } else {
                throw new SparkException(e);
            }
        });
    }
    // 网络消息[双向]
    public void postRemoteMessage(RequestMessage req, RpcResponseCallback callback) {
        RemoteNettyRpcCallContext callContext = new RemoteNettyRpcCallContext(req.senderAddress, nettyRpcEnv, callback);
        RpcMessage rpcMessage = new RpcMessage(req.senderAddress, req.content, callContext);
        postMessage(req.receiver.name(), rpcMessage, callback::onFailure);
    }
    // 广播消息
    public void postToAll(IndexMessage message) {
        Iterator<String> it = endPoints.keySet().iterator();
        while (it.hasNext()) {
            String pointName = it.next();
            postMessage(pointName, message, e -> {
                if (e instanceof RpcEnvStoppedException) {
                    LOGGER.debug("丢弃{}, 原因：{}", message, e.getMessage());
                } else  {
                    LOGGER.warn("丢弃{}, 原因：{}", message, e.getMessage());
                }
            });
        }
    }

    private void postMessage(String endPointName, IndexMessage message, ThrowableCallback callback) {
        Exception error = null;
        synchronized (this) {
            EndPointData data = endPoints.get(endPointName);
            if (stopped) {
                error = new RpcEnvStoppedException();
            } else if (data == null) {
                error = new SparkException(String.format("Could not find %s", endPointName));
            } else {
                data.index.post(message);
                receivers.offer(data);
            }
        }
        if (error != null) {
            callback.callbackIfStopped(error);
        }
    }

    public void awaitTermination() {
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public void stop(RpcEndPointRef rpcEndPointRef) {
        synchronized (this) {
            if (stopped) {
                return;
            }
            unregisterRpcEndpoint(rpcEndPointRef.name());
        }
    }

    public void stop() {
        synchronized (this) {
            stopped = true;
        }
        // 删除已注册的Rpc节点
        endPoints.keySet().forEach(this::unregisterRpcEndpoint);
        pool.shutdown();
    }

    private class EndPointData {
        String name;
        RpcEndPoint endPoint;
        RpcEndPointRef endPointRef;
        // RpcEndPoint与RpcEndPointRef间的消息收件箱
        Index index;

        EndPointData(String name, RpcEndPoint endPoint, RpcEndPointRef endPointRef) {
            this.name = name;
            this.endPoint = endPoint;
            this.endPointRef = endPointRef;
            this.index = new Index(this.endPoint, this.endPointRef);
        }
    }

    private class MessageLoop implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    EndPointData data = receivers.take();
                    data.index.process(Dispatcher.this);
                } catch (Exception e) {
                    LOGGER.error("thread = {} occur exception", Thread.currentThread().getName(), e);
                }
            }
        }
    }

    private interface ThrowableCallback {
        void callbackIfStopped(Exception e);
    }
}
