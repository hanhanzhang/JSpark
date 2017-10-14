package com.sdu.spark.rpc;

import java.io.File;

/**
 * @author hanhan.zhang
 * */
public interface RpcEnvFileServer {

    /**
     * Adds a file to be served by this RpcEnv. This is used to serve files from the driver
     * to executors when they're stored on the driver's local file system.
     *
     * @param file Local file to serve.
     * @return A URI for the location of the file.
     */
    String addFile(File file);

    /**
     * Adds a jar to be served by this RpcEnv. Similar to `addFile` but for jars added using
     * `SparkContext.addJar`.
     *
     * @param file Local file to serve.
     * @return A URI for the location of the file.
     */
    String addJar(File file);

    /**
     * Adds a local directory to be served via this file server.
     *
     * @param baseUri Leading URI path (files can be retrieved by appending their relative
     *                path to this base URI). This cannot be "files" nor "jars".
     * @param path Path to the local directory.
     * @return URI for the root of the directory in the file server.
     */
    String addDirectory(String baseUri, File path);
}
