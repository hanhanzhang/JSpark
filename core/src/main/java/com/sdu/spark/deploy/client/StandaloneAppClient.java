package com.sdu.spark.deploy.client;

import com.sdu.spark.deploy.ApplicationDescription;
import com.sdu.spark.deploy.DeployMessage.*;
import com.sdu.spark.deploy.ExecutorState;
import com.sdu.spark.deploy.Master;
import com.sdu.spark.rpc.*;
import com.sdu.spark.utils.DefaultFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.sdu.spark.utils.ThreadUtils.newDaemonCachedThreadPool;
import static com.sdu.spark.utils.ThreadUtils.newDaemonSingleThreadScheduledExecutor;
import static com.sdu.spark.utils.Utils.getFutureResult;

/**
 * {@link StandaloneAppClient}职责:
 *
 * 1: 向Spark Master注册Spark Job App
 *
 * 2: 向Spark Master申请Executor和关闭Executor
 *
 * 3: 同Spark Master资源管理交互
 *
 * @author hanhan.zhang
 * */
public class StandaloneAppClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandaloneAppClient.class);
    private int REGISTRATION_RETRIES = 3;

    private RpcEnv rpcEnv;
    private StandaloneAppClientListener listener;
    private SparkConf conf;

    /************************Spark Master信息**************************/
    private RpcAddress masterAddress;
    private String masterUrl;

    /************************Spark Job App信息*************************/
    // Spark Job App信息
    private ApplicationDescription appDescription;
    // Spark Job App唯一标识
    private AtomicReference<String> appId = new AtomicReference<>();
    private AtomicBoolean registered = new AtomicBoolean(false);

    // ClientEndPoint引用节点
    private AtomicReference<RpcEndPointRef> endpoint = new AtomicReference<>();


    public StandaloneAppClient(RpcEnv rpcEnv, String masterUrl, ApplicationDescription appDescription,
                               StandaloneAppClientListener listener, SparkConf conf) {
        this.rpcEnv = rpcEnv;
        this.masterUrl = masterUrl;
        this.masterAddress = RpcAddress.fromURI(this.masterUrl);
        this.appDescription = appDescription;
        this.listener = listener;
        this.conf = conf;
    }

    public void start() {
        endpoint.set(rpcEnv.setRpcEndPointRef("AppClient", new ClientEndPoint(rpcEnv)));
    }

    public void stop() {
        if (endpoint.get() != null) {
            Future<Boolean> future = endpoint.get().ask(new StopAppClient());
            boolean response = getFutureResult(future);
            if (response) {
                LOGGER.info("移除Application[name = {}]成功", appDescription.name);
            } else {
                LOGGER.error("移除Application[name = {}]失败", appDescription.name);
            }
            endpoint.set(null);
        }
    }

    /*****************************Spark Master申请Executor**************************/
    public Future<Boolean> requestTotalExecutors(int requestedTotal) {
        if (endpoint.get() != null && appId.get() != null) {
            return endpoint.get().ask(new RequestExecutors(appId.get(), requestedTotal));
        } else {
            LOGGER.error("在Driver完全启动后尝试申请Executor");
            return new DefaultFuture<>(false);
        }
    }


    /******************************Spark Master关闭Executor**************************/
    public Future<Boolean> killExecutors(List<String> executorIds) {
        if (endpoint.get() != null && appId.get() != null) {
            return endpoint.get().ask(new KillExecutors(appId.get(), executorIds));
        } else {
            LOGGER.error("在Driver完全启动后尝试关闭Executor");
            return new DefaultFuture<>(false);
        }
    }

    private class ClientEndPoint extends ThreadSafeRpcEndpoint {

        private RpcEndPointRef master = null;
        private boolean alreadyDisconnected = false;
        private AtomicReference<Boolean> alreadyDead = new AtomicReference<>(false);

        // 注册Spark Job App线程
        private ThreadPoolExecutor registerMasterThreadPool = newDaemonCachedThreadPool("appClient-register-master-thread", 1, 60);
        private ScheduledExecutorService registrationRetryThread =
                newDaemonSingleThreadScheduledExecutor("appClient-registration-retry-thread");
        private AtomicReference<Future<?>> registerMasterFuture = new AtomicReference<>();
        private AtomicReference<ScheduledFuture<?>> registrationRetryTimer = new AtomicReference<>();

        public ClientEndPoint(RpcEnv rpcEnv) {
            super(rpcEnv);
        }

        @Override
        public void onStart() {
            try {
                registerWithMaster(1);
            } catch (Exception e) {
                markDisconnected();
                stop();
            }
        }

        @Override
        public void receive(Object msg) {
            if (msg instanceof RegisteredApplication) {
                RegisteredApplication registeredApplication = (RegisteredApplication) msg;
                appId.set(registeredApplication.appId);
                master = registeredApplication.master;
                registered.set(true);
                listener.connected(registeredApplication.appId);
            } else if (msg instanceof ApplicationRemoved) {
                ApplicationRemoved applicationRemoved = (ApplicationRemoved) msg;
                markDead(String.format("Master removed our application: %s", applicationRemoved.message));
                stop();
            } else if (msg instanceof ExecutorAdded) {
                ExecutorAdded executorAdded = (ExecutorAdded) msg;
                String fullId = appId + "/" + executorAdded.execId;
                LOGGER.info("Master在工作节点Worker[workerId = {}, host = {}]启动Executor[fullId = {}, cores = {}]",
                            executorAdded.workerId, executorAdded.host, fullId, executorAdded.cores);
                listener.executorAdded(fullId, executorAdded.workerId, executorAdded.host, executorAdded.cores, executorAdded.memory);
            } else if (msg instanceof ExecutorUpdated) {
                ExecutorUpdated executor = (ExecutorUpdated) msg;
                String fullId = appId + "/" + executor.id;
                LOGGER.info("Executor[fullId = {}]状态更新: state = {}, message = {}",
                        fullId, executor.state, executor.message);
                if (ExecutorState.isFinished(executor.state)) {
                    listener.executorRemove(fullId, executor.message, executor.exitStatus, executor.workerLost);
                }
            } else if (msg instanceof WorkerRemoved) {
                WorkerRemoved workerRemoved = (WorkerRemoved) msg;
                LOGGER.info("Master移除工作节点Worker[workerId = {}]: {}", workerRemoved.id, workerRemoved.message);
                listener.workerRemoved(workerRemoved.id, workerRemoved.host, workerRemoved.message);
            } else if (msg instanceof MasterChanged) {
                MasterChanged masterChanged = (MasterChanged) msg;
                LOGGER.info("Master地址发生变更, 新地址: {}", masterChanged.master.address().toSparkURL());
                master = masterChanged.master;
                alreadyDisconnected = false;
                master.send(new MasterChangeAcknowledged(appId.get()));
            }
        }

        @Override
        public void receiveAndReply(Object msg, RpcCallContext context) {
            if (msg instanceof StopAppClient) {
                markDead("Application has been stopped.");
                sendToMaster(new UnregisterApplication(appId.get()));
                context.reply(true);
                stop();
            } else if (msg instanceof RequestExecutors) {
                if (master != null) {
                    askAndReplyAsync(master, context, msg);
                } else {
                    LOGGER.info("Spark App注册Master后尝试申请Executor");
                    context.reply(false);
                }
            } else if (msg instanceof KillExecutors) {
                if (master != null) {
                    askAndReplyAsync(master, context, msg);
                } else {
                    LOGGER.info("Spark App注册Master后尝试关闭Executor");
                    context.reply(false);
                }
            }
        }

        @Override
        public void onDisconnected(RpcAddress remoteAddress) {
            if (master.address().equals(remoteAddress)) {
                markDisconnected();
            }
        }

        /***************************Spark Master注册Spark App**********************/
        private void registerWithMaster(int nthRetry) {
            registerMasterFuture.set(tryRegisterMaster());
            registrationRetryTimer.set(registrationRetryThread.schedule(() -> {
                if (registered.get()) {
                    registerMasterFuture.get().cancel(true);
                    registerMasterThreadPool.shutdownNow();
                } else if (nthRetry >= REGISTRATION_RETRIES) {
                    markDead("Master is unresponsive! Giving up.");
                } else {
                    registerMasterFuture.get().cancel(true);
                    registerWithMaster(nthRetry + 1);
                }
            }, 20, TimeUnit.SECONDS));
        }
        private Future<?> tryRegisterMaster() {
            return registerMasterThreadPool.submit(() -> {
                if (registered.get()) {
                    return;
                }
                LOGGER.info("AppClient向Master注册Spark App[name = {}]", appDescription.name);
                RpcEndPointRef masterRef = rpcEnv.setRpcEndPointRef(Master.ENDPOINT_NAME, masterAddress);
                masterRef.send(new RegisterApplication(appDescription, self()));
            });
        }

        /*************************Spark Master发送消息************************/
        private void sendToMaster(Object message) {
            if (master != null) {
                master.send(message);
            } else {
                LOGGER.error("由于Master[address = {}]尚未连接丢弃消息: {}", masterAddress, message);
            }
        }

        private <T> void askAndReplyAsync(RpcEndPointRef endpointRef, RpcCallContext context, T message) {
            // Ask a message and create a thread to reply with the result.  Allow thread to be
            // interrupted during shutdown, otherwise context must be notified of NonFatal errors.
            Future<?> future = endpointRef.ask(message);
            getFutureResult(future, context);
        }

        private void markDisconnected() {
            if (!alreadyDisconnected) {
                listener.disconnected();
                alreadyDisconnected = true;
            }
        }

        private void markDead(String reason) {
            if (!alreadyDead.get()) {
                listener.dead(reason);
                alreadyDead.set(true);
            }
        }
    }
}
