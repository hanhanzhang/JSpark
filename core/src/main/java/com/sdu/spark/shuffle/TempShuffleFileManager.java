package com.sdu.spark.shuffle;

import java.io.File;

/**
 *
 * A manager to create temp shuffle block files to reduce the memory usage and also clean temp
 * files when they won't be used any more.
 *
 * @author hanhan.zhang
 * */
public interface TempShuffleFileManager {

    /**
     * 创建Shuffle Block文件
     * */
    File createTempShuffleFile();

    /**
     * Shuffle Block文件不再使用时, 应清理
     *
     * @return false : 需要自己清理文件
     * */
    boolean registerTempShuffleFileToClean(File file);

}
