package com.sdu.spark.storage;

import com.google.common.collect.Lists;
import com.sdu.spark.SparkException;
import com.sdu.spark.executor.ExecutorExitCode;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.utils.ShutdownHookManager;
import com.sdu.spark.utils.ShutdownHookManager.*;
import com.sdu.spark.utils.Utils;
import com.sdu.spark.utils.scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.sdu.spark.utils.Utils.*;

/**
 * Block数据落地磁盘文件管理
 *
 * 1: 根据Block Name路由落地磁盘文件
 *
 * 2: {@link DiskStore}负责将Block数据落地磁盘文件
 *
 * @author hanhan.zhang
 * */
public class DiskBlockManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiskBlockManager.class);

    public SparkConf conf;
    public boolean deleteFilesOnStop;

    public File[] localDirs;
    private File[][] subDirs;
    public int subDirsPerLocalDir;

    private SparkShutdownHook shutdownHook;

    public DiskBlockManager(SparkConf conf, boolean deleteFilesOnStop) {
        this.conf = conf;
        this.deleteFilesOnStop = deleteFilesOnStop;

        // 创建Block落地磁盘的目录
        localDirs = createLocalDirs(this.conf);
        if (localDirs.length == 0) {
            LOGGER.info("Failed to create any local dir.");
            System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR);
        }

        // 创建Block落地磁盘的目录文件
        subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64);
        subDirs = new File[localDirs.length][];
        for (int i = 0; i < localDirs.length; ++i) {
            subDirs[i] = new File[subDirsPerLocalDir];
        }

        // Shutdown hook
        this.shutdownHook = addShutdownHook();
    }

    private File[] createLocalDirs(SparkConf conf) {
        String[] dirs = getConfiguredLocalDirs(conf);
        File[] localDirs = new File[dirs.length];
        for (int i = 0; i < dirs.length; ++i) {
            try {
                localDirs[i] = createDirectory(dirs[i], "blockmgr");
                LOGGER.info("Created local directory at {}", localDirs[i]);
            } catch (IOException e) {
                LOGGER.error("Failed to create local dir in {}. Ignoring this directory.", dirs[i], e);
                localDirs[i] = null;
            }
        }
        return localDirs;
    }

    private File getFile(String filename) throws IOException {
        int hash = nonNegativeHash(filename);
        int dirId = hash % localDirs.length;
        int subDirId = (hash / localDirs.length) % subDirsPerLocalDir;

        File subDir;
        synchronized (subDirs[dirId]) {
            File old = subDirs[dirId][subDirId];
            if (old == null) {
                old = new File(localDirs[dirId], String.format("%02x", subDirId));
                if (!old.exists() && !old.mkdirs()) {
                    throw new IOException(String.format("Failed to create local dir in %s.", old));
                }
                subDirs[dirId][subDirId] = old;
            }
            subDir = old;
        }
        return new File(subDir, filename);
    }

    private SparkShutdownHook addShutdownHook() {
        LOGGER.debug("Adding shutdown hook");
        return ShutdownHookManager.get().add(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1, () -> {
            LOGGER.info("Shutdown hook called");
            DiskBlockManager.this.doStop();
        });
    }

    public File getFile(BlockId blockId) {
        try {
            return getFile(blockId.name());
        } catch (IOException e) {
            throw new SparkException(String.format("Got block %s store file failure", blockId), e);
        }
    }

    public boolean containsBlock(BlockId blockId) throws IOException {
        return getFile(blockId.name()).exists();
    }

    public File[] getAllFiles() {
        // Get all the files inside the array of array of directories
        List<File> blockFiles = Lists.newLinkedList();
        for (int i = 0;  i < subDirs.length; ++i) {
            synchronized (subDirs[i]) {
                for (int j = 0; j < subDirs[i].length; ++j) {
                    File subDir = subDirs[i][j];
                    if (subDir != null) {
                        blockFiles.addAll(Arrays.asList(subDir.listFiles()));
                    }
                }
            }
        }

        return blockFiles.toArray(new File[blockFiles.size()]);
    }

    public BlockId[] getAllBlocks() {
        File[] blockFiles = getAllFiles();
        BlockId[] blockIds = new BlockId[blockFiles.length];
        for (int i = 0; i < blockFiles.length; ++i) {
            blockIds[i] = BlockId.apply(blockFiles[i].getName());
        }
        return blockIds;
    }

    /** Produces a unique block id and File suitable for storing local intermediate results. */
    public Tuple2<BlockId, File> createTempLocalBlock() throws IOException {
        BlockId.TempLocalBlockId blockId = new BlockId.TempLocalBlockId(UUID.randomUUID());
        while (getFile(blockId).exists()) {
            blockId = new BlockId.TempLocalBlockId(UUID.randomUUID());
        }
        return new Tuple2<>(blockId, getFile(blockId));
    }

    /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
    public Tuple2<BlockId, File> createTempShuffleBlock() {
        BlockId.TempShuffleBlockId blockId = new BlockId.TempShuffleBlockId(UUID.randomUUID());
        while (getFile(blockId).exists()) {
            blockId = new BlockId.TempShuffleBlockId(UUID.randomUUID());
        }
        return new Tuple2<>(blockId, getFile(blockId));
    }

    public void stop() {
        ShutdownHookManager.get().removeShutdownHook(shutdownHook);
        doStop();
    }

    private void doStop() {
        if (deleteFilesOnStop) {
            for (File localDir : localDirs) {
                if (localDir.isDirectory() && localDir.exists()) {
                    try {
                        if (!ShutdownHookManager.get().hasRootAsShutdownDeleteDir(localDir)) {
                            Utils.deleteRecursively(localDir);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Exception while deleting local spark dir: {}", localDir, e);
                    }
                }
            }
        }
    }
}
