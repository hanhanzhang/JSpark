package com.sdu.spark.rpc.netty;


import com.google.common.collect.Maps;
import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcConfig;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.utils.ThreadUtils;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * {@link Dispatcher}路由消息给适合的{@link }
 *
 * @author hanhan.zhang
 * */
public class Dispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);

    private NettyRpcEnv nettyRpcEnv;

    /**
     * key = Rpc节点名, value = Rpc节点
     * */
    private Map<String, EndPointData> endPoints = Maps.newConcurrentMap();

    /**
     * key = Rpc节点, value = Rpc节点引用
     * */
    private Map<RpcEndPoint, RpcEndPointRef> endPointRefs = Maps.newConcurrentMap();

    /**
     *
     * */
    private LinkedBlockingQueue<EndPointData> receivers = new LinkedBlockingQueue<>();


    private Boolean stopped = false;

    /**
     * 消息分发工作线程
     * */
    private ThreadPoolExecutor pool;

    private static final int DEFAULT_DISPATCHER_THREADS = 5;

    public Dispatcher(NettyRpcEnv nettyRpcEnv, RpcConfig rpcConfig) {
        this.nettyRpcEnv = nettyRpcEnv;

        int threads = rpcConfig.getDispatcherThreads();
        if (threads <= 0) {
            threads = DEFAULT_DISPATCHER_THREADS;
        }
        pool = ThreadUtils.newDaemonCachedThreadPool("dispatcher-event-loop-%d", threads, 60);
        for (int i = 0; i < threads; ++i) {
            pool.execute(new MessageLoop());
        }
    }

    /**
     * 注册Rpc节点,并返回该节点的引用
     * */
    public NettyRpcEndPointRef registerRpcEndPoint(String name, RpcEndPoint endPoint) {
        RpcAddress address = nettyRpcEnv.address();
        NettyRpcEndPointRef endPointRef = new NettyRpcEndPointRef(name, address, nettyRpcEnv);
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

    /**
     * 返回Rpc节点的引用
     * */
    public RpcEndPointRef getRpcEndPointRef(RpcEndPoint endPoint) {
        return endPointRefs.get(endPoint);
    }

    public void removeRpcEndPointRef(RpcEndPoint endPoint) {
        endPointRefs.remove(endPoint);
    }


    private void postMessage(String endPointName, IndexMessage message) {
        EndPointData data = endPoints.get(endPointName);
        synchronized (this) {
            if (stopped) {
                throw new IllegalStateException("RpcEnv already stopped.");
            }
            data.index.post(message);
        }
    }

    public void awaitTermination() {
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
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

    @Getter
    private class EndPointData {
        private String name;
        private RpcEndPoint endPoint;
        private RpcEndPointRef endPointRef;

        private Index index;

        EndPointData(String name, RpcEndPoint endPoint, RpcEndPointRef endPointRef) {
            this.name = name;
            this.endPoint = endPoint;
            this.endPointRef = endPointRef;
            this.index = new Index(this.endPoint, this.endPointRef);
        }
    }

    /**
     * 消息任务
     * */
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
}
