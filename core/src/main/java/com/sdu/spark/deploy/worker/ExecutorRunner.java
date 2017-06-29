package com.sdu.spark.deploy.worker;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.deploy.ApplicationDescription;
import com.sdu.spark.deploy.DeployMessage.ExecutorStateChanged;
import com.sdu.spark.deploy.ExecutorState;
import com.sdu.spark.rpc.JSparkConfig;
import com.sdu.spark.rpc.RpcEndPointRef;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static com.sdu.spark.deploy.worker.CommandUtils.redirectStream;

/**
 * 启动Executor进程
 *
 * @author hanhan.zhang
 * */
public class ExecutorRunner {
    public String appId;
    public int execId;
    public ApplicationDescription appDesc;
    public int cores;
    public int memory;
    public RpcEndPointRef worker;
    public File sparkHome;
    public File executorDir;
    public JSparkConfig conf;
    public String[] appLocalDirs;
    public ExecutorState state;

    private Thread workerThread;
    private Process process;

    public ExecutorRunner(String appId, int execId, ApplicationDescription appDesc, int cores, int memory, RpcEndPointRef worker, File sparkHome, File executorDir, JSparkConfig conf, String[] appLocalDirs, ExecutorState state) {
        this.appId = appId;
        this.execId = execId;
        this.appDesc = appDesc;
        this.cores = cores;
        this.memory = memory;
        this.worker = worker;
        this.sparkHome = sparkHome;
        this.executorDir = executorDir;
        this.conf = conf;
        this.appLocalDirs = appLocalDirs;
        this.state = state;
    }

    public void start() {
        workerThread = new Thread(() -> {
            try {
                fetchAndRunExecutor();
            } catch (IOException e) {
                // ignore
            } catch (InterruptedException e) {
                // ignore interrupt
            }
        }, String.format("ExecutorRunner for %s", fullId()));
        workerThread.start();

        // Todo: ShutdownHook
    }

    private String fullId() {
        return  appId + "/" + execId;
    }

    private void fetchAndRunExecutor() throws IOException, InterruptedException {
        // 创建Executor进程
        ProcessBuilder builder = CommandUtils.buildProcessBuilder(appDesc.command, new SecurityManager(conf),
                memory, sparkHome.getAbsolutePath(), new String[0], "", Collections.emptyMap());
        builder.directory(executorDir);
        builder.environment().put("SPARK_EXECUTOR_DIRS", StringUtils.join(appLocalDirs, File.separator));
        builder.environment().put("SPARK_LAUNCH_WITH_SCALA", "0");

        process = builder.start();

        // 重定向Executor进程输入/输出
        File stdout = new File(executorDir, "stdout");
        redirectStream(process.getInputStream(), stdout);
        File stderr = new File(executorDir, "stderr");
        redirectStream(process.getErrorStream(), stderr);


        // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
        // or with nonzero exit code
        int exitCode = process.waitFor();
        state = ExecutorState.EXITED;

        worker.send(new ExecutorStateChanged(execId, appId, state, "", exitCode));
    }
}
