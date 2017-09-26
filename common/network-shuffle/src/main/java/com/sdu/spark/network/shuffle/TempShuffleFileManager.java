package com.sdu.spark.network.shuffle;

import java.io.File;

/**
 * A manager to create temp shuffle block files to reduce the memory usage and also clean temp
 * files when they won't be used any more.
 */
public interface TempShuffleFileManager {


    /** Create a temp shuffle block file. */
    File createTempShuffleFile();

    /**
     * Register a temp shuffle file to clean up when it won't be used any more. Return whether the
     * file is registered successfully. If `false`, the caller should clean up the file by itself.
     */
    boolean registerTempShuffleFileToClean(File file);

}
