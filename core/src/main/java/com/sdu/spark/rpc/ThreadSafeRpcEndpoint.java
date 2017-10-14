package com.sdu.spark.rpc;

import com.sdu.spark.rpc.netty.Dispatcher;

/**
 * requires RpcEnv thread-safely sending messages to it
 *
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same {@link ThreadSafeRpcEndpoint}. In the other words, changes to internal fields of a
 * {@link ThreadSafeRpcEndpoint} are visible when processing the next message, and fields in the
 * {@link ThreadSafeRpcEndpoint} need not be volatile or equivalent.
 *
 * However, there is no guarantee that the same thread will be executing the same
 * {@link ThreadSafeRpcEndpoint} for different messages.
 *
 * Note：
 *
 *  {@link com.sdu.spark.rpc.netty.Index#process(Dispatcher)}启动时, 确保只有单线程访问, 即只有一个线程访问
 *
 * @author hanhan.zhang
 * */
public abstract class ThreadSafeRpcEndpoint extends RpcEndPoint {

    public ThreadSafeRpcEndpoint(RpcEnv rpcEnv) {
        super(rpcEnv);
    }

}
