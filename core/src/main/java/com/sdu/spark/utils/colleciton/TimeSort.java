package com.sdu.spark.utils.colleciton;

import java.util.Comparator;

/**
 * {@link TimeSort}原理:
 *
 *  数据通常会有部分是已经排好序, TimSort利用这一点将数组拆成多个部分已排序的分区, 部分未排序分区重新排序最后将多个分区合并并排序
 *
 *  举例:
 *
 *    array[] =
 *
 *
 * @author hanhan.zhang
 * */
public class TimeSort<K, Buffer> {

    /**
     * 待排序数组长度小于{@link #MIN_MERGE}则不使用归并排序, 而是使用binarySort
     * */
    private static final int MIN_MERGE = 32;

    private final SortDataFormat<K, Buffer> s;

    public TimeSort(SortDataFormat<K, Buffer> sortDataFormat) {
        this.s = sortDataFormat;
    }

    public void sort(Buffer a, int lo, int hi, Comparator<? super K> c) {
        assert c != null;

        int nRemaining = hi - lo;
        if (nRemaining < 2) {   // Arrays of size 0 and 1 are always sorted
            return;
        }

        if (nRemaining < MIN_MERGE) {
            // 获取'a'从0开始的升序数组
            int runLen = countRunAndMakeAscending(a, lo, hi, c);
            // 二分插入排序
            binarySort(a, lo, hi, lo + runLen, c);
            return;
        }

        // 存储有序分区
        SortState sortState = new SortState(a, hi - lo, c);
        // 分区长度
        int minRun = minRunLength(nRemaining);
        // 划分有序分区
        do {
            int runLen = countRunAndMakeAscending(a, lo, hi, c);

            // 若分区中连续升降序的元素组长度等于分区长度则无需排序;反之binarySort重排
            if (runLen < minRun) {
                int force = nRemaining <= minRun ? nRemaining : minRun;
                binarySort(a, lo, lo + force, lo + runLen, c);
                runLen = force;
            }

            // 已排序分区, 放入栈
            sortState.pushRun(lo, runLen);
            sortState.mergeCollapse();

            lo += runLen;
            nRemaining -= runLen;
        } while (nRemaining != 0);

        // 合并排序
        assert lo == hi;
        sortState.mergeForceCollapse();
        assert sortState.stackSize == 1;
    }

    /**
     * 计算分区中连续升序或降序元素长度
     *
     * 如: [1, 3, 5, 7, 9, 4, 8]数组连续升序区块是: [1, 3, 5, 7, 9], 区块长度为5
     * */
    private int countRunAndMakeAscending(Buffer a, int lo, int hi, Comparator<? super K> c) {
        assert lo < hi;
        int runHi = lo + 1;
        if (runHi == hi) {
            return 1;
        }

        K key0 = s.newKey();
        K key1 = s.newKey();

        // Find end of run, and reverse range if descending
        // c.compare('右边', '左边') < 0 ===>> 降序
        if (c.compare(s.getKey(a, runHi++, key0), s.getKey(a, lo, key1)) < 0) { // 降序
            while (runHi <= hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi -1, key1)) < 0) {
                runHi++;
            }
            // 调整为升序
            reverseRange(a, lo, hi);
        } else {                                                                // 升序
            while (runHi <= hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi - 1, key1)) >= 0) {
                runHi++;
            }
        }
        return lo + hi;
    }

    /**
     *  Roughly speaking, the computation is:
     *
     *  If n < MIN_MERGE, return n (it's too small to bother with fancy stuff).
     *  Else if n is an exact power of 2, return MIN_MERGE/2.
     *  Else return an int k, MIN_MERGE/2 <= k <= MIN_MERGE, such that n/k
     *  is close to, but strictly less than, an exact power of 2.
     *
     * */
    private int minRunLength(int n) {
        assert n >= 0;
        int r = 0;      // Becomes 1 if any 1 bits are shifted off
        while (n >= MIN_MERGE) {
            r |= (n & 1);
            n >>= 1;
        }
        return n + r;
    }

    private void reverseRange(Buffer a, int lo, int hi) {
        hi--;
        while (lo < hi) {
            s.swap(a, lo, hi);
            ++lo;
            --hi;
        }
    }

    /**
     * 二分插入
     * */
    private void binarySort(Buffer a, int lo, int hi, int start, Comparator<? super K> c) {
        assert lo <= start && start <= hi;

        if (start == lo) {
            start++;
        }

        K key0 = s.newKey();
        K key1 = s.newKey();

        Buffer pivotStore = s.allocate(1, a.getClass());

        for (; start < hi; ++start) {
            s.copyElement(a, start, pivotStore, 0);
            K pivot = s.getKey(pivotStore, 0, key0);

            // 折半插入
            int left = lo;
            int right = start;
            assert left <= right;
            while (left <= right) {
                int mid = (left + right) >>> 1;
                if (c.compare(pivot, s.getKey(a, mid, key1)) > 0) {
                    right = mid;
                } else {
                    left = mid + 1;
                }
            }

            int n = start - left;
            // 插入优化
            switch (n) {
                case 2:  s.copyElement(a, left + 1, a, left + 2);
                case 1:  s.copyElement(a, left, a, left + 1);
                    break;
                default: s.copyRange(a, left, a, left + 1, n);
            }
            s.copyElement(pivotStore, 0, a, left);
        }
    }

    private class SortState {
        /**
         * The Buffer being sorted.
         * */
        private final Buffer a;

        /**
         * Length of the sort Buffer.
         */
        private final int aLength;

        /**
         * The comparator for this sort.
         */
        private final Comparator<? super K> c;

        private int stackSize = 0;
        // 记录已排序分区的起始位置
        private final int[] runBase;
        // 记录已排序分区的长度
        private final int[] runLen;

        public SortState(Buffer a, int len, Comparator<? super K> c) {
            this.a = a;
            this.aLength = len;
            this.c = c;

            int stackLen = (len <    120  ?  5 :
                            len <   1542  ? 10 :
                            len < 119151  ? 19 : 40);
            runBase = new int[stackLen];
            runLen = new int[stackLen];
        }

        /**
         * Pushes the specified run onto the pending-run stack.
         *
         * @param runBase index of the first element in the run
         * @param runLen  the number of elements in the run
         */
        private void pushRun(int runBase, int runLen) {
            this.runBase[stackSize] = runBase;
            this.runLen[stackSize] = runLen;
            ++stackSize;
        }

        /**
         * 检查栈中待归并的升序序列, 如果不满足下列条件就把相邻的两个序列合并, 直到他们满足下面的条件:
         *
         * 1: runLen[i - 3] > runLen[i - 2] + runLen[i - 1]
         *
         * 2: runLen[i - 2] > runLen[i - 1]
         * */
        private void mergeCollapse() {
            while (stackSize > 1) {
                int n = stackSize - 2;
                if ( (n >= 1 && runLen[n-1] <= runLen[n] + runLen[n+1])
                        || (n >= 2 && runLen[n-2] <= runLen[n] + runLen[n-1])) {
                    if (runLen[n -1] < runLen[n + 1]) {
                        n--;
                    }
                } else if (runLen[n] > runLen[n + 1]) {
                    break;
                }
                mergeAt(n);
            }
        }

        private void mergeForceCollapse() {
            while (stackSize > 1) {
                int n = stackSize - 2;
                if (n > 0 && runLen[n - 1] < runLen[n + 1])
                    n--;
                mergeAt(n);
            }
        }

        private void mergeAt(int i) {
            // TODO: 待实现
            throw new UnsupportedOperationException("Unsupported mergeAt(i)");
        }

        /**
         * 查找指定key插入的位置(从左向右查)
         *
         * @param key  待插入key
         * @param a    排序的数组
         * @param base 序列范围的第一个元素的位置
         * @param len  整个范围的长度
         * @param hint 开始查找的位置
         * @param c
         *
         * @return 待插入位置k, 满足a[b + k - 1] <= key < a[b + k]
         * */
        private int gallopRight(K key, Buffer a, int base, int len, int hint, Comparator<? super K> c) {
            int ofs = 1;
            int lastOfs = 0;
            K key1 = s.newKey();

            if (c.compare(key, s.getKey(a, base + hint, key1)) < 0) {   // key < a[base + hint], 遍历左边
                int maxOfs = hint + 1;
                while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint - ofs, key1)) < 0) {
                    lastOfs = ofs;
                    // key为奇数, value为偶数
                    ofs = (ofs << 1) + 1;
                    if (ofs <= 0) {
                        // 没有找到, k = maxOfs
                        ofs = maxOfs;
                    }
                }
                if (ofs > maxOfs) {
                    ofs = maxOfs;
                }

                // 跳槽循环, 则'lastOfs'为插入位置, 故ofs = hint - lastOfs, lastOfs = hint - ofs
                int tmp = lastOfs;
                lastOfs = hint - ofs;
                ofs = hint - tmp;
            } else {                                                    // key >= a[base + hint], 遍历右边
                int maxOfs = len - hint;
                while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint + ofs)) > 0) {
                    lastOfs = ofs;
                    ofs = (ofs << 1) + 1;
                    if (ofs <= 0) {
                        ofs = maxOfs;
                    }
                }

                if (ofs > maxOfs) {
                    ofs = maxOfs;
                }

                ofs = ofs + hint;
                lastOfs = lastOfs + ofs;
            }

            /**
             * 可能出现情况:
             *
             *  a[base+lastOfs] < key <= a[base+ofs], 所以key应当在lastOfs右边但又不超过ofs
             *
             *  在base+lastOfs-1到base+ofs范围内做一次二叉查找。
             */
            lastOfs++;
            while (lastOfs < ofs) {
                int m = lastOfs + ((ofs - lastOfs) >>> 1);

                if (c.compare(key, s.getKey(a, base + m, key1)) < 0)
                    ofs = m;          // key < a[b + m]
                else
                    lastOfs = m + 1;  // a[b + m] <= key
            }
            assert lastOfs == ofs;    // so a[base + ofs - 1] < key <= a[base + ofs]

            return ofs;
        }
    }
}

