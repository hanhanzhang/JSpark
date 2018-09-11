package com.sdu.spark.rpc.netty;

import com.google.common.collect.Maps;
import com.sdu.spark.network.buffer.FileSegmentManagedBuffer;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.server.StreamManager;
import com.sdu.spark.rpc.RpcEnvFileServer;
import com.sdu.spark.utils.Utils;

import java.io.File;
import java.util.Map;

import static com.sdu.spark.utils.Utils.stripPrefix;
import static java.lang.String.format;

/**
 * @author hanhan.zhang
 * */
public class NettyStreamManager extends StreamManager implements RpcEnvFileServer {

    private NettyRpcEnv nettyEnv;

    private final Map<String, File> files;
    private final Map<String, File> jars;
    private final Map<String, File> dirs;

    public NettyStreamManager(NettyRpcEnv nettyEnv) {
        this.nettyEnv = nettyEnv;

        this.files = Maps.newConcurrentMap();
        this.jars = Maps.newConcurrentMap();
        this.dirs = Maps.newConcurrentMap();
    }

    @Override
    public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ManagedBuffer openStream(String streamId) {
        String[] array = stripPrefix(streamId, "/").split("/");
        File file;
        switch (array[0]) {
            case "files":
                file = files.get(array[1]);
                break;
            case "jars":
                file = jars.get(array[1]);
                break;
            default:
                File dir = dirs.get(array[0]);
                assert dir != null : "Invalid stream URI: " + array[0] + " not found.";
                file = new File(dir, array[1]);
        }

        if (file != null && file.isFile()) {
            return new FileSegmentManagedBuffer(nettyEnv.getTransportConf(), file, 0, file.length());
        }
        return null;
    }

    @Override
    public String addFile(File file) {
        File existingPath = files.putIfAbsent(file.getName(), file);
        assert existingPath == null || existingPath == file : format("File %s was already registered with a different path " +
                "(old path = %s, new path = %s", file.getName(), existingPath, file);
        return format("%s/files/%s", nettyEnv.address().toSparkURL(), Utils.encodeFileNameToURIRawPath(file.getName()));
    }

    @Override
    public String addJar(File file) {
        File existingPath = jars.putIfAbsent(file.getName(), file);
        assert existingPath == null || existingPath == file : format("File %s was already registered with a different path " +
                "(old path = %s, new path = %s", file.getName(), existingPath, file);
        return format("%s/jars/%s", nettyEnv.address().toSparkURL(), Utils.encodeFileNameToURIRawPath(file.getName()));
    }

    @Override
    public String addDirectory(String baseUri, File path) {
        String fixedBaseUri = validateDirectoryUri(baseUri);
        File existingPath = dirs.putIfAbsent(stripPrefix(fixedBaseUri, "/"), path);
        assert existingPath == null : format("URI '%s' already registered.", fixedBaseUri);
        return format("%s%s", nettyEnv.address().toSparkURL(), fixedBaseUri);
    }
}
