package com.sdu.spark.deploy;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.JSparkConfig;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.utils.CommandUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 *  Todo:
 *  {@link ProcessBuilder}使用
 *
 * @author hanhan.zhang
 * */
public class DriverRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverRunner.class);

    private JSparkConfig conf;
    private String driverId;
    private File workDir;
    private File sparkHome;
    private DriverDescription desc;
    private RpcEndPointRef worker;
    private SecurityManager securityManager;

    /**
     * Driver进程
     * */
    private volatile Process process;
    private volatile boolean killed;

    private volatile DriverState finishedState;
    private volatile Exception finishedException;

    public DriverRunner(JSparkConfig conf, String driverId, File workDir, File sparkHome, DriverDescription desc,
                        RpcEndPointRef worker, SecurityManager securityManager) {
        this.conf = conf;
        this.driverId = driverId;
        this.workDir = workDir;
        this.sparkHome = sparkHome;
        this.desc = desc;
        this.worker = worker;
        this.securityManager = securityManager;
    }

    public void start() {
        new Thread(() -> {
            try {
                int exitCode = prepareAndRunDriver();
                if (exitCode == 0) {
                    finishedState = DriverState.FINISHED;
                } else if (killed) {
                    finishedState = DriverState.KILLED;
                } else {
                    finishedState = DriverState.FAILED;
                }
            } catch (Exception e) {
                kill();
                finishedState = DriverState.ERROR;
                finishedException = e;
            }
        }, "DriverRunner for " + driverId).start();
    }

    private void kill() {
        LOGGER.info("关闭Driver(driverId = {})进程", driverId);
        killed = true;
        process.destroy();
    }

    private int prepareAndRunDriver() throws IOException, InterruptedException {
        File driverDir = createWorkingDirectory();
        /**
         * Todo: Jar文件下载
         * */
        String localJarFilename = downloadUserJar(driverDir);
        ProcessBuilder builder = CommandUtils.buildProcessBuilder(desc.command, securityManager, desc.mem,
                sparkHome.getAbsolutePath(), new String[0], localJarFilename, Collections.emptyMap());
        return runDriver(builder, sparkHome, desc.supervise);
    }

    private int runDriver(ProcessBuilder builder, File baseDir, boolean supervise) throws IOException, InterruptedException {
        builder.directory(baseDir);
        return runCommandWithRetry(builder, supervise);
    }

    private int runCommandWithRetry(ProcessBuilder builder, boolean supervise) throws IOException, InterruptedException {
        int exitCode = -1;
        boolean keepTrying = !killed;
        while (keepTrying) {
            synchronized (this) {
                if (killed) {
                    return exitCode;
                }
                process = builder.start();
                initialize(process);
            }
        }
        return process.waitFor();
    }

    /**
     * 初始化进程输入输出文件
     * */
    private void initialize(Process process) throws IOException {
        File stdout = new File(sparkHome, "stdout");
        CommandUtils.redirectStream(process.getInputStream(), stdout);

        File stderr = new File(sparkHome, "stderr");
        CommandUtils.redirectStream(process.getErrorStream(), stderr);
    }

    private File createWorkingDirectory() {
        File driverDir = new File(workDir, driverId);
        if (!driverDir.exists() && !driverDir.mkdirs()) {
            throw new RuntimeException("Failed to create directory " + driverDir);
        }
        return driverDir;
    }

    /**
     * Todo:
     * */
    private String downloadUserJar(File driverDir) {
        return "";
    }


}
