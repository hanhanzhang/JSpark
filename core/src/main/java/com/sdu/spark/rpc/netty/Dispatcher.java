package com.sdu.spark.rpc.netty;


import com.google.common.collect.Maps;
import com.sdu.spark.SparkException;
import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.rpc.*;
import com.sdu.spark.utils.ThreadUtils;
import com.sdu.spark.rpc.netty.IndexMessage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

/**
 * {@link Dispatcher}负责路由接收到的消息[本地消息及网络消息]给{@link RpcEndpoint}
 *
 * @author hanhan.zhang
 * */
public class Dispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);

    /**
     * {@link EndPointData}职责:
     *
     *  1: 维护{@link RpcEndpoint}与{@link RpcEndpointRef}对应关系
     *
     *  2: 维护{@link RpcEndpoint}与{@link RpcEndpointRef}Rpc消息接收信箱{@link Index}
     *
     *  3: {@link EndPointData}实例化时, 向信箱投递{@link OnStart}消息, 进而调用{@link RpcEndpoint#onStart()}方法
     * */
    private class EndPointData {
        String name;
        RpcEndpoint endPoint;
        RpcEndpointRef endPointRef;
        Index index;

        EndPointData(String name, RpcEndpoint endPoint, RpcEndpointRef endPointRef) {
            this.name = name;
            this.endPoint = endPoint;
            this.endPointRef = endPointRef;
            this.index = new Index(this.endPoint, this.endPointRef);
        }
    }

    private EndPointData PoisonPill = new EndPointData(null, null, null);

    private NettyRpcEnv nettyRpcEnv;

    /*****************************Spark Point-To-Point映射*******************************/
    /** key = RpcEndpoint Name, value = EndPointData*/
    private Map<String, EndPointData> endPoints = Maps.newConcurrentMap();
    /**key = RpcEndpoint, value = RpcEndpointRef*/
    private Map<RpcEndpoint, RpcEndpointRef> endPointRefs = Maps.newConcurrentMap();
    /**Track the receivers whose inboxes may contain messages(线程池访问, 需确保线程安全)*/
    private LinkedBlockingQueue<EndPointData> receivers = new LinkedBlockingQueue<>();

    private Boolean stopped = false;

    /**Rpc Message分发线程池*/
    private ThreadPoolExecutor threadPool;

    private class MessageLoop implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    EndPointData data = receivers.take();
                    if (data == PoisonPill) {
                        // Put PoisonPill back so that other MessageLoops can see it.
                        receivers.offer(PoisonPill);
                        return;
                    }
                    data.index.process(Dispatcher.this);
                } catch (Exception e) {
                    LOGGER.error("thread = {} occur exception", Thread.currentThread().getName(), e);
                }
            }
        }
    }

    /**
     * @param numUsableCores Number of CPU cores allocated to the process, for sizing the thread threadPool.
     *                       If 0, will consider the available CPUs on the host.
     * */
    public Dispatcher(NettyRpcEnv nettyRpcEnv, int numUsableCores) {
        this.nettyRpcEnv = nettyRpcEnv;

        int availableCores = numUsableCores > 0 ? numUsableCores
                                                : Runtime.getRuntime().availableProcessors();
        int numThreads = nettyRpcEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
                                                 Math.max(availableCores, 2));
        threadPool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop-%d");
        // 启动消息处理任务
        for (int i = 0; i < numThreads; ++i) {
            threadPool.execute(new MessageLoop());
        }
    }


    /*********************************Spark Point-To-Point映射管理************************************/
    // RpcEndPoint节点注册, 并返回RpcEndPoint节点的引用
    public NettyRpcEndpointRef registerRpcEndPoint(String name, RpcEndpoint endPoint) {
        RpcEndpointAddress endpointAddress = new RpcEndpointAddress(name, nettyRpcEnv.address());
        NettyRpcEndpointRef endPointRef = new NettyRpcEndpointRef(endpointAddress, nettyRpcEnv);
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

    /**RpcEndpoint Name查询, {@link RpcEndpointVerifier#receiveAndReply(Object, RpcCallContext)} */
    public RpcEndpointRef verify(String name) {
        EndPointData endPointData = endPoints.get(name);
        if (endPointData == null) {
            return null;
        }
        return endPointData.endPointRef;
    }

    public RpcEndpointRef getRpcEndPointRef(RpcEndpoint endPoint) {
        return endPointRefs.get(endPoint);
    }

    public void removeRpcEndPointRef(RpcEndpoint endPoint) {
        endPointRefs.remove(endPoint);
    }

    /** 本地消息*/
    public Object postLocalMessage(RequestMessage req) {
        try {
            CompletableFuture<Object> p = new CompletableFuture<>();
            LocalNettyRpcCallContext callContext = new LocalNettyRpcCallContext(req.senderAddress, p);
            RpcMessage rpcMessage = new RpcMessage(req.senderAddress, req.content, callContext);
            postMessage(req.receiver.name(), rpcMessage, null);
            return p.get();
        } catch (Exception e) {
            throw new SparkException("Got local message result failure", e);
        }
    }

    /**网络消息[单向]*/
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

    /**网络消息[双向]*/
    public void postRemoteMessage(RequestMessage req, RpcResponseCallback callback) {
        RemoteNettyRpcCallContext callContext = new RemoteNettyRpcCallContext(req.senderAddress, nettyRpcEnv, callback);
        RpcMessage rpcMessage = new RpcMessage(req.senderAddress, req.content, callContext);
        postMessage(req.receiver.name(), rpcMessage, callback::onFailure);
    }

    /**广播消息*/
    public void postToAll(IndexMessage message) {
        Iterator<String> it = endPoints.keySet().iterator();
        while (it.hasNext()) {
            String pointName = it.next();
            postMessage(pointName, message, e -> {
                if (e instanceof RpcEnvStoppedException) {
                    LOGGER.debug("Message {} dropped", message, e.getMessage());
                } else  {
                    LOGGER.warn("Message {} dropped", message, e.getMessage());
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
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public void stop(RpcEndpointRef rpcEndpointRef) {
        synchronized (this) {
            if (stopped) {
                return;
            }
            unregisterRpcEndpoint(rpcEndpointRef.name());
        }
    }

    public void stop() {
        synchronized (this) {
            stopped = true;
        }
        // 删除已注册的Rpc节点
        endPoints.keySet().forEach(this::unregisterRpcEndpoint);
        receivers.offer(PoisonPill);
        threadPool.shutdown();
    }

    private interface ThrowableCallback {
        void callbackIfStopped(Exception e);
    }
}
