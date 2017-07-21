package com.sdu.spark.laucher;

import com.sdu.spark.launcher.LauncherConnection;
import com.sdu.spark.launcher.LauncherProtocol;
import com.sdu.spark.launcher.LauncherProtocol.*;
import com.sdu.spark.launcher.SparkAppHandle.*;
import com.sdu.spark.utils.ThreadUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ThreadFactory;

/**
 *
 * todo: LauncherServer
 *
 * @author hanhan.zhang
 * */
public abstract class LauncherBackend {
    private static final Logger LOGGER = LoggerFactory.getLogger(LauncherBackend.class);

    private static final ThreadFactory threadFactory = ThreadUtils.namedThreadFactory("LauncherBackend", true);

    private Thread clientThread;
    private BackendConnection connection;
    private State lastState;

    private volatile boolean isConnected = false;

    public void connect() {
        int port = NumberUtils.toInt(System.getenv().get(LauncherProtocol.ENV_LAUNCHER_PORT));
        String secret = System.getenv().get(LauncherProtocol.ENV_LAUNCHER_SECRET);

        if (StringUtils.isNotEmpty(secret)) {
            // 创建连接
            try {
                Socket s = new Socket(InetAddress.getLoopbackAddress(), port);
                connection = new BackendConnection(s);
                connection.send(new Hello(secret, "1.0"));
                clientThread = LauncherBackend.threadFactory.newThread(connection);
                clientThread.start();
                isConnected = true;
            } catch (IOException e) {
                LOGGER.error("LauncherBackend连接异常", e);
            }
        }
    }

    public void close() {
        try {
            if (connection != null) {
                try {
                    connection.close();
                } finally {
                    if (clientThread != null) {
                        clientThread.join();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("LauncherBackend关闭异常", e);
        }
    }

    public void setAppId(String appId) {
        if (connection != null) {
            try {
                connection.send(new SetAppId(appId));
            } catch (Exception e) {
                LOGGER.error("注册AppId({})给LauncherServer异常", appId, e);
            }
        }
    }

    public void setState(State state) {
        try {
            if (connection != null && lastState != state) {
                connection.send(new SetState(state));
                lastState = state;
            }
        } catch (Exception e) {
            LOGGER.error("向LauncherServer发送State异常", e);
        }
    }

    private void fireStopRequest() {
        Thread thread = threadFactory.newThread(() -> {
            try {
                onStopRequest();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
    }

    public abstract void onStopRequest();

    private class BackendConnection extends LauncherConnection {

        BackendConnection(Socket socket) throws IOException {
            super(socket);
        }

        @Override
        protected void handle(Message msg) throws IOException {
            if (msg instanceof Stop) {
                fireStopRequest();
            } else {
                throw new IllegalArgumentException(String.format("Unexpected message type: %s", msg.getClass().getName()));
            }
        }
    }
}
