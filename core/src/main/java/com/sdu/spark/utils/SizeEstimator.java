package com.sdu.spark.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.sdu.spark.utils.colleciton.OpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

/**
 * 估算Java Object内存占用量
 *
 * Based on the following JavaWorld article:
 * http://www.javaworld.com/javaworld/javaqa/2003-12/02-qa-1226-sizeof.html
 *
 * @author hanhan.zhang
 * */
public class SizeEstimator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SizeEstimator.class);

    private static ConcurrentMap<Class<?>, ClassInfo> classInfos = new MapMaker().weakKeys().makeMap();

    /**Java基本类型占用字节数(long, int, short, byte)*/
    private static ArrayList<Integer> fieldSizes = Lists.newArrayList(8, 4, 2, 1);
    private static final int BYTE_SIZE = 1;
    private static final int BOOLEAN_SIZE = 1;
    private static final int CHAR_SIZE = 2;
    private static final int SHORT_SIZE = 2;
    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;
    private static final int FLOAT_SIZE = 4;
    private static final int DOUBLE_SIZE = 8;

    /**对象引用占用字节*/
    private static int pointerSize = 4;
    private static int objectSize = 8;

    /**8字节对齐*/
    private static final int ALIGN_SIZE = 8;

    // Estimate the size of arrays larger than ARRAY_SIZE_FOR_SAMPLING by sampling.
    private static final int ARRAY_SIZE_FOR_SAMPLING = 400;
    private static int ARRAY_SAMPLE_SIZE = 100; // should be lower than ARRAY_SIZE_FOR_SAMPLING

    private static boolean is64bit = false;
    // Size of an object reference
    // Based on https://wikis.oracle.com/display/HotSpotInternals/CompressedOops
    private static boolean isCompressedOops = false;

    static {
        String arch = System.getProperty("os.arch");
        is64bit = arch.contains("64") || arch.contains("s390x");
        isCompressedOops = getIsCompressedOops();

        objectSize = !is64bit ? 8 : !isCompressedOops ? 16 : 12;
        pointerSize = is64bit && !isCompressedOops ? 8 : 4;
        classInfos.clear();
        classInfos.put(Object.class, new ClassInfo(objectSize, null));
    }

    private static boolean getIsCompressedOops() {
        // This is only used by tests to override the detection of compressed oops. The test
        // actually uses a system property instead of a SparkConf, so we'll stick with that.
        if (System.getProperty("spark.test.useCompressedOops") != null) {
            return toBoolean(System.getProperty("spark.test.useCompressedOops"));
        }

        // java.vm.info provides compressed ref info for IBM JDKs
        if (System.getProperty("java.vendor").contains("IBM")) {
            return System.getProperty("java.vm.info").contains("Compressed Ref");
        }

        try {
            String hotSpotMBeanName = "com.sun.management:type=HotSpotDiagnostic";
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();

            // NOTE: This should throw an exception in non-Sun JVMs
            // scalastyle:off classforname
            Class<?> hotSpotMBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
            Method getVMMethod = hotSpotMBeanClass.getDeclaredMethod("getVMOption",
                    Class.forName("java.lang.String"));
            // scalastyle:on classforname

            Object bean = ManagementFactory.newPlatformMXBeanProxy(server,
                    hotSpotMBeanName, hotSpotMBeanClass);
            // TODO: We could use reflection on the VMOption returned ?
            return getVMMethod.invoke(bean, "UseCompressedOops").toString().contains("true");
        } catch (Exception e) {
            // Guess whether they've enabled UseCompressedOops based on whether maxMemory < 32 GB
            boolean guess = Runtime.getRuntime().maxMemory() < (32L*1024*1024*1024);
            String guessInWords = guess ? "yes" : "not";
            LOGGER.info("Failed to check whether UseCompressedOops is set; assuming {}", guessInWords);
            return guess;
        }
    }

    public static long estimate(Object obj) {
        return estimate(obj, new IdentityHashMap<>());
    }

    public static long estimate(Object obj, IdentityHashMap<Object, Object> visited) {
        SearchState state = new SearchState(visited);
        state.enqueue(obj);
        while (!state.isFinished()) {
            visitSingleObject(state.dequeue(), state);
        }
        return state.size;
    }

    private static void visitSingleObject(Object obj, SearchState state) {
        Class<?> cls = obj.getClass();
        if (cls.isArray()) {
            visitArray(obj, cls, state);
        } else if (cls.getName().startsWith("java.lang.reflect")) {

        } else if (obj instanceof ClassLoader || obj instanceof Class) {

        } else if (obj instanceof KnownSizeEstimation){
            state.size += ((KnownSizeEstimation) obj).estimatedSize();
        } else {
            ClassInfo classInfo = getClassInfo(cls);
            state.size += alignSize(classInfo.shellSize);
            for (Field field : classInfo.pointerFields) {
                try {
                    state.enqueue(field.get(obj));
                } catch (IllegalAccessException e) {
                    // ignore
                }
            }
        }
    }

    private static void visitArray(Object array, Class<?> arrayClass, SearchState state) {
        int length = ((Object[]) array).length;
        Class<?> elementClass = arrayClass.getComponentType();

        // Arrays have object header and length field which is an integer
        long arrSize = alignSize(objectSize + INT_SIZE);

        if (elementClass.isPrimitive()) {
            arrSize += alignSize(length * primitiveSize(elementClass));
            state.size += arrSize;
        } else {
            arrSize += alignSize(length * pointerSize);
            state.size += arrSize;

            if (length <= ARRAY_SIZE_FOR_SAMPLING) {
                Object[] objArray = (Object[]) array;
                int arrayIndex = 0;
                while (arrayIndex < length) {
                    state.enqueue(objArray[arrayIndex]);
                    arrayIndex += 1;
                }
            } else {
                // Estimate the size of a large array by sampling elements without replacement.
                // To exclude the shared objects that the array elements may link, sample twice
                // and use the min one to calculate array size.
                Random rand = new Random(42);
                OpenHashSet<Integer> drawn = new OpenHashSet<>(2 * ARRAY_SAMPLE_SIZE);
                long s1 = sampleArray(array, state, rand, drawn, length);
                long s2 = sampleArray(array, state, rand, drawn, length);
                long size = Math.min(s1, s2);
                state.size += Math.max(s1, s2) +
                        (size * ((length - ARRAY_SAMPLE_SIZE) / (ARRAY_SAMPLE_SIZE)));
            }
        }
    }

    private static ClassInfo getClassInfo(Class<?> cls) {
        // Check whether we've already cached a ClassInfo for this class
        ClassInfo info = classInfos.get(cls);
        if (info != null) {
            return info;
        }

        ClassInfo parent = getClassInfo(cls.getSuperclass());
        long shellSize = parent.shellSize;
        List<Field> pointerFields = parent.pointerFields;

        // 计算每个字节出现次数
        int[] sizeCount = sizeCount();

        // iterate through the fields of this class and gather information.
        for (Field field : cls.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                Class<?> fieldClass = field.getType();
                if (fieldClass.isPrimitive()) {
                    sizeCount[primitiveSize(fieldClass)] += 1;
                } else {
                    field.setAccessible(true);
                    sizeCount[pointerSize] += 1;
                    pointerFields.add(field);
                }
            }
        }

        // Based on the simulated field layout code in Aleksey Shipilev's report:
        // http://cr.openjdk.java.net/~shade/papers/2013-shipilev-fieldlayout-latest.pdf
        // The code is in Figure 9.
        // The simplified idea of field layout consists of 4 parts (see more details in the report):
        //
        // 1. field alignment: HotSpot lays out the fields aligned by their size.
        // 2. object alignment: HotSpot rounds instance size up to 8 bytes
        // 3. consistent fields layouts throughout the hierarchy: This means we should layout
        // superclass first. And we can use superclass's shellSize as a starting point to layout the
        // other fields in this class.
        // 4. class alignment: HotSpot rounds field blocks up to HeapOopSize not 4 bytes, confirmed
        // with Aleksey. see https://bugs.openjdk.java.net/browse/CODETOOLS-7901322
        //
        // The real world field layout is much more complicated. There are three kinds of fields
        // order in Java 8. And we don't consider the @contended annotation introduced by Java 8.
        // see the HotSpot classloader code, layout_fields method for more details.
        // hg.openjdk.java.net/jdk8/jdk8/hotspot/file/tip/src/share/vm/classfile/classFileParser.cpp
        long alignedSize = shellSize;
        for (int size : fieldSizes) {
            if (sizeCount[size] <= 0) {
                continue;
            }
            long count = sizeCount[size];
            // If there are internal gaps, smaller field can fit in.
            alignedSize = Math.max(alignedSize, alignSizeUp(shellSize, size) + size * count);
            shellSize += size * count;
        }

        // Should choose a larger size to be new shellSize and clearly alignedSize >= shellSize, and
        // round up the instance filed blocks
        shellSize = alignSizeUp(alignedSize, pointerSize);

        // Create and cache a new ClassInfo
        ClassInfo newInfo = new ClassInfo(shellSize, pointerFields);
        classInfos.put(cls, newInfo);
        return newInfo;
    }

    private static long sampleArray(Object array, SearchState state, Random random, OpenHashSet<Integer> drawn, int length) {
        long size = 0L;
        for (int i = 0; i < ARRAY_SAMPLE_SIZE; ++i) {
            int index = 0;
            do {
                index = random.nextInt(length);
            } while (drawn.contains(index));
            drawn.add(index);
            Object[] objArray = (Object[]) array;
            Object obj = objArray[index];
            if (obj != null) {
                size += estimate(obj, state.visited);
            }
        }
        return size;
    }

    private static int[] sizeCount() {
        int max = Collections.max(fieldSizes);
        return new int[max + 1];
    }

    private static long alignSize(long size) {
        return alignSizeUp(size, ALIGN_SIZE);
    }

    /**
     * Compute aligned size. The alignSize must be 2^n, otherwise the result will be wrong.
     * When alignSize = 2^n, alignSize - 1 = 2^n - 1. The binary representation of (alignSize - 1)
     * will only have n trailing 1s(0b00...001..1). ~(alignSize - 1) will be 0b11..110..0. Hence,
     * (size + alignSize - 1) & ~(alignSize - 1) will set the last n bits to zeros, which leads to
     * multiple of alignSize.
     */
    private static long alignSizeUp(long size, int alignSize) {
        return (size + alignSize - 1) & ~(alignSize - 1);
    }

    private static int primitiveSize(Class<?> cls) {
        if (cls == Byte.class) {
            return BYTE_SIZE;
        } else if (cls == Boolean.class) {
            return BOOLEAN_SIZE;
        } else if (cls == Character.class) {
            return CHAR_SIZE;
        } else if (cls == Short.class) {
            return SHORT_SIZE;
        } else if (cls == Integer.class) {
            return INT_SIZE;
        } else if (cls == Long.class) {
            return LONG_SIZE;
        } else if (cls == Float.class) {
            return FLOAT_SIZE;
        } else if (cls == Double.class) {
            return DOUBLE_SIZE;
        } else {
            throw new IllegalArgumentException(
                    "Non-primitive class " + cls + " passed to primitiveSize()");
        }
    }

    private static class ClassInfo {
        long shellSize;
        List<Field> pointerFields;

        ClassInfo(long shellSize, List<Field> pointerFields) {
            this.shellSize = shellSize;
            this.pointerFields = pointerFields;
        }
    }

    /**
     * 估算Java Object及Object引用属性辅助类
     * */
    private static class SearchState {
        private IdentityHashMap<Object, Object> visited;

        private Stack<Object> stack;
        private long size = 0L;

        SearchState(IdentityHashMap<Object, Object> visited) {
            this.visited = visited;
            this.stack = new Stack<>();
        }

        private void enqueue(Object object) {
            stack.push(object);
        }

        private Object dequeue() {
            return stack.pop();
        }

        private boolean isFinished() {
            return stack.isEmpty();
        }
    }

    public interface KnownSizeEstimation {
        long estimatedSize();
    }
}
