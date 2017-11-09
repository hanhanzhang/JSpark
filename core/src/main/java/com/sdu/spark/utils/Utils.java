package com.sdu.spark.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.sdu.spark.SparkException;
import com.sdu.spark.network.utils.ByteUnit;
import com.sdu.spark.network.utils.JavaUtils;
import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.utils.scala.Tuple2;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.math.NumberUtils.toInt;

/**
 * @author hanhan.zhang
 * */
public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    private static boolean isWindows = SystemUtils.IS_OS_WINDOWS;
    private static boolean isMac = SystemUtils.IS_OS_MAC;
    private static int MAX_DIR_CREATION_ATTEMPTS = 10;

    private static final ImmutableMap<String, TimeUnit> timeSuffixes = ImmutableMap.<String, TimeUnit> builder()
                                                                                    .put("us", TimeUnit.MICROSECONDS)
                                                                                    .put("ms", TimeUnit.MILLISECONDS)
                                                                                    .put("s", TimeUnit.SECONDS)
                                                                                    .put("m", TimeUnit.MINUTES)
                                                                                    .put("min", TimeUnit.MINUTES)
                                                                                    .put("h", TimeUnit.HOURS)
                                                                                    .put("d", TimeUnit.DAYS)
                                                                                    .build();

    private static volatile String[] localRootDirs = null;

    private static Map<String, Tuple2<String, Integer>> hostPortParseResults = Maps.newConcurrentMap();
    private static String customHostname = System.getenv("SPARK_LOCAL_HOSTNAME");


    public static String exceptionString(Throwable e) {
        if (e == null) {
            return "";
        } else {
            // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
            StringWriter stringWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stringWriter));
            return stringWriter.toString();
        }
    }

    public static int nonNegativeMod(int x, int mod) {
        int rawMod = x % mod;
        return rawMod + (rawMod < 0 ? mod : 0);
    }

    public static void checkHost(String host) {
        assert host != null && host.indexOf(':') == -1 :
                String.format("Expected hostname (not IP) but got %s", host);
    }

    public static void setCustomHostname(String hostname) {
        // DEBUG code
        Utils.checkHost(hostname);
        customHostname = hostname;
    }

    public static Tuple2<String, Integer> parseHostPort(String hostPort) {
        // Check cache first.
        Tuple2<String, Integer> cached = hostPortParseResults.get(hostPort);
        if (cached != null) {
            return cached;
        }

        int index = hostPort.lastIndexOf(':');
        // This is potentially broken - when dealing with ipv6 addresses for example, sigh ...
        // but then hadoop does not support ipv6 right now.
        // For now, we assume that if port exists, then it is valid - not check if it is an int > 0
        if (-1 == index) {
            Tuple2<String, Integer> retval = new Tuple2<>(hostPort, 0);
            hostPortParseResults.put(hostPort, retval);
            return retval;
        }

        Tuple2<String, Integer> retval = new Tuple2<>(hostPort.substring(0, index).trim(), toInt(hostPort.substring(index + 1).trim()));
        hostPortParseResults.putIfAbsent(hostPort, retval);
        return hostPortParseResults.get(hostPort);
    }

    public static int terminateProcess(Process process, long timeoutMs)  {
        try {
            // Politely destroy first
            process.destroy();
            if (process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
                // Successful exit
                return process.exitValue();
            } else {
                process.destroyForcibly();
                if (process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
                    return process.exitValue();
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("Exception when attempting to kill process", e);
        }
        return -1;
    }

    public static String megabytesToString(long megabytes) {
        return bytesToString(megabytes * 1024L * 1024L);
    }

    public static <T> T getFutureResult(Future<T> future) {
        try {
            return future.get();
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

    public static void getFutureResult(Future<?> future, RpcCallContext context) {
        Object response;
        try {
            response = future.get();
            context.reply(response);
        } catch (Exception e) {
           context.sendFailure(e);
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

    public static void writeByteBuffer(ByteBuffer bb, DataOutput out) throws IOException {
        if (bb.hasArray()) {
            out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
        } else {
            int originalPosition = bb.position();
            byte[] remainBytes = new byte[bb.remaining()];
            bb.get(remainBytes);
            out.write(remainBytes);
            bb.position(originalPosition);
        }
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

    public static URI resolveURI(String path) {
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

    public static long timeStringAs(String str, TimeUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();

        try {
            Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
            if (!m.matches()) {
                throw new NumberFormatException("Failed to parse time string: " + str);
            }

            long val = Long.parseLong(m.group(1));
            String suffix = m.group(2);

            // Check for invalid suffixes
            if (suffix != null && !timeSuffixes.containsKey(suffix)) {
                throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
            }

            // If suffix is valid use that, otherwise none was provided and use the default passed
            return unit.convert(val, suffix != null ? timeSuffixes.get(suffix) : unit);
        } catch (NumberFormatException e) {
            String timeError = "Time must be specified as seconds (s), " +
                    "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
                    "E.g. 50s, 100ms, or 250us.";

            throw new NumberFormatException(timeError + "\n" + e.getMessage());
        }
    }

    public static long computeTotalGcTime() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                                    .map(GarbageCollectorMXBean::getCollectionTime).count();
    }

    public static Class<?> classForName(String className){
        try {
            return Class.forName(className, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    public static boolean isLocalMaster(SparkConf conf) {
        String master = conf.get("spark.master", "");
        return master.equals("local") || master.startsWith("local[");
    }

    public static String localHostName() {
        try {
            return findLocalInetAddress().getHostAddress();
        } catch (Exception e) {
            return "localhost";
        }
    }

    private static InetAddress findLocalInetAddress() throws Exception {
        String defaultIpOverride = System.getenv("SPARK_LOCAL_IP");
        if (defaultIpOverride != null) {
            return InetAddress.getByName(defaultIpOverride);
        } else {
           return InetAddress.getLocalHost();
        }
    }

    public static long byteStringAsMb(String str) {
        return JavaUtils.byteStringAs(str, ByteUnit.MiB);
    }

    public static long byteStringAsKb(String str) {
        return JavaUtils.byteStringAs(str, ByteUnit.KiB);
    }

    public static String bytesToString(long size) {
        return bytesToString(BigInteger.valueOf(size));
    }

    private static String bytesToString(BigInteger size) {
        long EB = 1L << 60;
        long PB = 1L << 50;
        long TB = 1L << 40;
        long GB = 1L << 30;
        long MB = 1L << 20;
        long KB = 1L << 10;

        if (size.compareTo(BigInteger.valueOf(1L << 11 * EB)) > 0) {
            // The number is too large, show it in scientific notation.
            return new BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B";
        } else {
            if (size.compareTo(BigInteger.valueOf(2 * EB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(EB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "EB");
            } else if (size.compareTo(BigInteger.valueOf(2 * PB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(PB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "PB");
            } else if (size.compareTo(BigInteger.valueOf(2 * TB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(TB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "TB");
            } else if (size.compareTo(BigInteger.valueOf(2 * GB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(GB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "GB");
            } else if (size.compareTo(BigInteger.valueOf(2 * MB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(MB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "MB");
            } else if (size.compareTo(BigInteger.valueOf(2 * KB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(KB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "KB");
            } else {
                return String.format("%s.1f %s", size.toString(), "B");
            }
        }
    }

    private static boolean isRunningInYarnContainer(SparkConf conf) {
        // These environment variables are set by YARN.
        return conf.getenv("CONTAINER_ID") != null;
    }

    private static String[] getOrCreateLocalRootDirs(SparkConf conf) {
        if (localRootDirs == null) {
            synchronized (Utils.class) {
                if (localRootDirs == null) {
                    localRootDirs = getOrCreateLocalRootDirsImpl(conf);
                }
            }

        }
        return localRootDirs;
    }

    private static String[] getOrCreateLocalRootDirsImpl(SparkConf conf) {
        String[] localDirs = getConfiguredLocalDirs(conf);
        String[] localDirPaths = new String[localDirs.length];
        for (int i = 0; i < localDirs.length; ++i) {
            try {
                File rootDir = new File(localDirs[i]);
                if (rootDir.exists() || rootDir.mkdirs()) {
                    File dir = createTempDir(localDirs[i]);
                    chmod700(dir);
                    localDirPaths[i] = dir.getAbsolutePath();
                } else {
                    LOGGER.error("Failed to create dir in {}. Ignoring this directory.", localDirs[i]);
                    localDirPaths[i] = null;
                }
            } catch (IOException e) {
                LOGGER.error("Failed to create local root dir in {}. Ignoring this directory.", localDirs[i]);
                localDirPaths[i] = null;
            }
        }
        return localDirPaths;
    }

    private static boolean chmod700(File file) {
        return file.setReadable(false, false) &&
                file.setReadable(true, true) &&
                file.setWritable(false, false) &&
                file.setWritable(true, true) &&
                file.setExecutable(false, false) &&
                file.setExecutable(true, true);
    }

    public static File createTempDir() throws IOException {
        return createTempDir("spark");
    }

    private static File createTempDir(String namePrefix) throws IOException {
        return createTempDir(System.getProperty("java.io.tmpdir"), namePrefix);
    }

    public static File createTempDir(String root, String namePrefix) throws IOException {
        File dir = createDirectory(root, namePrefix);
        ShutdownHookManager.get().registerShutdownDeleteDir(dir);
        return dir;
    }


    private static String getYarnLocalDirs(SparkConf conf) {
        String localDirs = conf.getenv("LOCAL_DIRS");

        if (Strings.isEmpty(localDirs)) {
            throw new RuntimeException("Yarn Local dirs can't be empty");
        }
        return localDirs;
    }

    public static String[] getConfiguredLocalDirs(SparkConf conf) {
        boolean shuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false);
        if (isRunningInYarnContainer(conf)) {
            // If we are in yarn mode, systems can have different disk layouts so we must set it
            // to what Yarn on this system said was available. Note this assumes that Yarn has
            // created the directories already, and that they are secured so that only the
            // user has access to them.
            return getYarnLocalDirs(conf).split(",");
        } else if (conf.getenv("SPARK_EXECUTOR_DIRS") != null) {
            return conf.getenv("SPARK_EXECUTOR_DIRS").split(File.pathSeparator);
        } else if (conf.getenv("SPARK_LOCAL_DIRS") != null) {
            return conf.getenv("SPARK_LOCAL_DIRS").split(",");
        } else if (conf.getenv("MESOS_DIRECTORY") != null && !shuffleServiceEnabled) {
            // Mesos already creates a directory per Mesos task. Spark should use that directory
            // instead so all temporary files are automatically cleaned up when the Mesos task ends.
            // Note that we don't want this if the shuffle service is enabled because we want to
            // continue to serve shuffle files after the executors that wrote them have already exited.
            return new String[]{conf.getenv("MESOS_DIRECTORY")};
        } else {
            if (conf.getenv("MESOS_DIRECTORY") != null && shuffleServiceEnabled) {
                LOGGER.info("MESOS_DIRECTORY available but not using provided Mesos sandbox because " +
                        "spark.shuffle.service.enabled is enabled.");
            }
            // In non-Yarn mode (or for the driver in yarn-client mode), we cannot trust the user
            // configuration to point to a secure directory. So create a subdirectory with restricted
            // permissions under each listed directory.
            return conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")).split(",");
        }
    }

    public static File createDirectory(String root, String namePrefix) throws IOException {
        int attempts = 0;
        int maxAttempts = MAX_DIR_CREATION_ATTEMPTS;
        File dir = null;
        while (dir == null) {
            attempts += 1;
            if (attempts > maxAttempts) {
                throw new IOException("Failed to create a temp directory (under " + root + ") after " +
                        maxAttempts + " attempts!");
            }
            try {
                dir = new File(root, namePrefix + "-" + UUID.randomUUID().toString());
                if (dir.exists() || !dir.mkdirs()) {
                    dir = null;
                }
            } catch(SecurityException e) {
                dir = null;
            }
        }

        return dir.getCanonicalFile();
    }

    public static int nonNegativeHash(Object obj) {

        // Required ?
        if (obj == null) return 0;

        int hash = obj.hashCode();
        // math.abs fails for Int.MinValue
        return Integer.MIN_VALUE != hash ? Math.abs(hash) : 0;
    }

    public static String getLocalDir(SparkConf conf) {
        String[] localRootDirs = getOrCreateLocalRootDirs(conf);
        String localDir = localRootDirs[0];
        if (StringUtils.isEmpty(localDir)) {
            String[] configuredLocalDirs = getConfiguredLocalDirs(conf);
            throw new SparkException("Failed to get a temp directory under " + StringUtils.join(configuredLocalDirs, ","));
        }
        return localDir;
    }

    public static void deleteRecursively(File file) throws IOException {
        if (file != null) {
            try {
                if (file.isDirectory() && !isSymlink(file)) {
                    IOException savedIOException = null;
                    File[] childFiles = listFilesSafely(file);
                    for (int i = 0; i < childFiles.length; ++i) {
                        try {
                            deleteRecursively(childFiles[i]);
                        } catch (IOException e) {
                            savedIOException = e;
                        }
                    }
                    if (savedIOException != null) {
                        throw savedIOException;
                    }
                    ShutdownHookManager.get().removeShutdownDeleteDir(file);
                }
            } finally {
                if (file.delete()) {
                    LOGGER.trace("{} has been deleted", file.getAbsoluteFile());
                } else {
                    if (file.exists()) {
                        throw new IOException("Failed to delete: " + file.getAbsolutePath());
                    }
                }
            }
        }
    }

    private static boolean isSymlink(File file){
        return Files.isSymbolicLink(Paths.get(file.toURI()));
    }

    private static File[] listFilesSafely(File file) throws IOException {
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files == null) {
                throw new IOException("Failed to list files for dir: " + file);
            }
            return files;
        } else {
            return new File[0];
        }
    }

    public static File tempFileWith(File path) {
        return new File(path.getAbsolutePath() + "." + UUID.randomUUID());
    }

    public static String getUsedTimeMs(long startTimeMs) {
        return (System.currentTimeMillis() - startTimeMs) + " ms";
    }
}
