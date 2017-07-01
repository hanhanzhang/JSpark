package com.sdu.spark.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * @author hanhan.zhang
 * */
public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    private static boolean isWindows = SystemUtils.IS_OS_WINDOWS;
    private static boolean isMac = SystemUtils.IS_OS_MAC;

    public static <T> T getFutureResult(Future<?> future) {
        try {
            return (T) future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.error("future task interrupted exception", e);
            throw new RuntimeException("future task interrupted exception", e);
        } catch (ExecutionException e) {
            LOGGER.error("future task execute exception", e);
            e.printStackTrace();
            throw new RuntimeException("future task execute exception", e);
        }
    }


    public static int convertStringToInt(String text) {
        return Integer.parseInt(text);
    }

    public static String libraryPathEnvName() {
        if (isWindows) {
            return "PATH";
        } else if (isMac) {
            return "DYLD_LIBRARY_PATH";
        } else {
            return "LD_LIBRARY_PATH";
        }
    }

    public static long copyStream(InputStream input, OutputStream out, boolean transferToEnabled) throws IOException {
        long count = 0;
        try {
            if (input instanceof FileInputStream && out instanceof FileOutputStream && transferToEnabled) {
                FileChannel inputChannel = ((FileInputStream) input).getChannel();
                FileChannel outputChanel = ((FileOutputStream) out).getChannel();
                count = inputChannel.size();
                copyFileStreamNIO(inputChannel, outputChanel, 0, count);
            } else {
                byte[] buf = new byte[8192];
                int n = 0;
                while (n != -1) {
                    n = input.read(buf);
                    if (n != -1) {
                        out.write(buf, 0, n);
                        count += n;
                    }
                }
            }
        } finally {
            if (input != null) {
                input.close();
            }
            if (out != null) {
                out.close();
            }
        }
        return count;
    }

    private static void copyFileStreamNIO(FileChannel input, FileChannel out, int startPosition, long bytesToCopy) throws IOException {
        long initialPos = out.position();
        long count = 0L;
        while (count < bytesToCopy) {
            count += input.transferTo(count + startPosition, bytesToCopy - count, out);
        }
        assert count == bytesToCopy :
                String.format("需要复制%s字节数据, 实际复制%s字节数据", bytesToCopy, count);
        long finalPos = out.position();
        long expectedPos = initialPos + bytesToCopy;
        assert finalPos == expectedPos :
                String.format("Current position %s do not equal to expected position %s", finalPos, expectedPos);
    }

    public static String resolveURIs(String paths) {
        if (paths == null || paths.trim().isEmpty()) {
            return "";
        } else {
            List<URI> uriList = Arrays.stream(paths.split(",")).filter(p -> !p.isEmpty()).map(Utils::resolveURI).collect(Collectors.toList());
            return StringUtils.join(uriList, ",");
        }
    }

    private static URI resolveURI(String path) {
        try {
            URI uri = new URI(path);
            if (uri.getScheme() != null) {
                return uri;
            }
            if (uri.getFragment() != null) {
                URI absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI();
                return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
                        uri.getFragment());
            }
        } catch (Exception e) {
            // ignore
        }
        return new File(path).getAbsoluteFile().toURI();
    }
}
