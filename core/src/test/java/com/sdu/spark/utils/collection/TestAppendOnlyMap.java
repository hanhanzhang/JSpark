package com.sdu.spark.utils.collection;

import com.sdu.spark.utils.colleciton.AppendOnlyMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author hanhan.zhang
 * */
public class TestAppendOnlyMap {

    @BeforeClass
    public static void testStart() {
        System.out.println("test start");
    }

    @Test
    public void testInitialize() {
        AppendOnlyMap<Integer, Integer> appendOnlyMap = new AppendOnlyMap<>(33);
        assert appendOnlyMap.capacity() == 64;

        appendOnlyMap = new AppendOnlyMap<>(32);
        assert appendOnlyMap.capacity() == 32;
    }

    @Test
    public void testMapGrowTable() {
        AppendOnlyMap<Integer, Integer> appendOnlyMap = new AppendOnlyMap<>(4);
        assert appendOnlyMap.capacity() == 4;

        // 4 * 0.75 = 3
        appendOnlyMap.update(1, 10);
        appendOnlyMap.update(2, 12);
        // appendOnlyMap需扩容
        appendOnlyMap.update(3, 13);
        appendOnlyMap.update(4, 14);
        assert appendOnlyMap.capacity() == 4 * 2;
    }

    @Test
    public void testMapGet() {
        AppendOnlyMap<Integer, Integer> appendOnlyMap = new AppendOnlyMap<>(5);
        assert appendOnlyMap.capacity() == 8;

        appendOnlyMap.update(1, 29);
        appendOnlyMap.update(2, 32);

        int value = appendOnlyMap.apply(2);
        assert value == 32;

        Integer v = appendOnlyMap.apply(5);
        assert v == null;
    }

    @Test
    public void testMapUpdate() {
        AppendOnlyMap<Integer, Integer> appendOnlyMap = new AppendOnlyMap<>(1 << 20);

        appendOnlyMap.update(2, 56);
        assert appendOnlyMap.apply(2) == 56;

        AppendOnlyMap.Updater<Integer> updater = (hadValue, value) -> {
            if (hadValue) {
                return value + 2;
            } else {
                return 60;
            }
        };
        appendOnlyMap.changeValue(2, updater);
        assert appendOnlyMap.apply(2) == 58;

        appendOnlyMap.changeValue(3, updater);
        assert appendOnlyMap.apply(3) == 60;

    }

    @AfterClass
    public static void testFinish() {
        System.out.println("test finished");
    }
}
