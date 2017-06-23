package com.sdu.spark.deploy;

import com.sdu.spark.rpc.JSparkConfig;
import com.sdu.spark.rpc.RpcEndPointRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 *
 *
 * @author hanhan.zhang
 * */
public class DriverRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverRunner.class);

    private JSparkConfig conf;
    private String driverId;
    private File workDir;
    private File sparkHome;
    private DriverDescription driverDesc;
    private RpcEndPointRef worker;
    private SecurityManager securityManager;

    /**
     * Driver进程
     * */
    private volatile Process process;
    private volatile boolean killed;

    private volatile DriverState finishedState;
    private volatile Exception finishedException;

    public DriverRunner(JSparkConfig conf, String driverId, File workDir, File sparkHome, DriverDescription driverDesc,
                        RpcEndPointRef worker, SecurityManager securityManager) {
        this.conf = conf;
        this.driverId = driverId;
        this.workDir = workDir;
        this.sparkHome = sparkHome;
        this.driverDesc = driverDesc;
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

    private int prepareAndRunDriver() {
        File driverDir = createWorkingDirectory();
        String localJarFilename = downloadUserJar(driverDir);
        return 0;
    }

    private File createWorkingDirectory() {
        File driverDir = new File(workDir, driverId);
        if (!driverDir.exists() && !driverDir.mkdirs()) {
            throw new RuntimeException("Failed to create directory " + driverDir);
        }
        return driverDir;
    }

    private String downloadUserJar(File driverDir) {
        return "";
    }


}
