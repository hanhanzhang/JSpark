package com.sdu.spark.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.sdu.spark.unfase.memory.MemoryBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.*;

import static com.sdu.spark.utils.Utils.bytesToString;

/**
 * {@link TaskMemoryManager}职责:
 *
 * 1:
 *
 * 2:
 *
 * @author hanhan.zhang
 * */
public class TaskMemoryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskMemoryManager.class);

    public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;
    private static final int PAGE_NUMBER_BITS = 13;
    private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;
    private static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51
    /** Bit mask for the lower 51 bits of a long. */
    private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

    private long taskAttemptId;

    // 内存块池, 每个内存块称之为"内存页"
    private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];
    // track free memory block
    private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);
    // 已分配内存尚未用的内存块数
    private volatile long acquiredButNotUsed = 0L;
    // 内存申请类型
    final MemoryMode tungstenMemoryMode;
    // 内存管理器
    private final MemoryManager memoryManager;

    private final HashSet<MemoryConsumer> consumers;

    public TaskMemoryManager(MemoryManager manager, long taskId) {
        this.memoryManager = manager;
        this.tungstenMemoryMode = this.memoryManager.tungstenMemoryMode;
        this.taskAttemptId = taskId;
        this.consumers = Sets.newHashSet();
    }

    public long acquireExecutionMemory(long required, MemoryConsumer consumer) {
        assert required >= 0;
        assert consumer != null;
        MemoryMode memoryMode = consumer.getMode();

        synchronized (this) {
            long got = memoryManager.acquireExecutionMemory(required, taskAttemptId, memoryMode);
            if (got < required) {
                // 分配的内存小于申请的内存, 则看是否能够释放其他Task已分配内存, 以此减少Spill到文件
                TreeMap<Long, List<MemoryConsumer>> sortedConsumers = new TreeMap<>();
                consumers.forEach(c -> {
                    if (c != consumer && c.used > 0 && c.getMode() == memoryMode) {
                        long key = c.used;
                        List<MemoryConsumer> cList = sortedConsumers.putIfAbsent(key, new ArrayList<>(1));
                        cList.add(c);
                    }
                });

                while (!sortedConsumers.isEmpty()) {
                    // 选择已分配内存至少大于required-got的Consume释放内存
                    Map.Entry<Long,List<MemoryConsumer>> currentEntry = sortedConsumers.ceilingEntry(required - got);
                    if (currentEntry == null) {
                        // 选择最大占用内存的Consumer释放内存
                        currentEntry = sortedConsumers.lastEntry();
                    }

                    // 释放内存并重新分配
                    List<MemoryConsumer> cList = currentEntry.getValue();
                    MemoryConsumer c = cList.remove(cList.size() - 1);
                    if (cList.isEmpty()) {
                        sortedConsumers.remove(currentEntry.getKey());
                    }
                    try {
                        long released = c.spill(required - got, consumer);
                        if (released > 0) {
                            LOGGER.debug("Task {} released {} from {} for {}", taskAttemptId,
                                    bytesToString(released), c, consumer);
                            got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, memoryMode);
                            if (got >= required) {
                                break;
                            }
                        }
                    } catch (ClosedByInterruptException e) {
                        // This called by user to kill a task (e.g: speculative task).
                        LOGGER.error("error while calling spill() on " + c, e);
                        throw new RuntimeException(e.getMessage());
                    } catch (IOException e) {
                        LOGGER.error("error while calling spill() on " + c, e);
                        throw new OutOfMemoryError("error while calling spill() on " + c + " : "
                                + e.getMessage());
                    }
                }
            }

            // call spill() on itself
            if (got < required) {
                try {
                    long released = consumer.spill(required - got, consumer);
                    if (released > 0) {
                        LOGGER.debug("Task {} released {} from itself ({})", taskAttemptId, bytesToString(released), consumer);
                        got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, memoryMode);
                    }
                } catch (ClosedByInterruptException e) {
                    // This called by user to kill a task (e.g: speculative task).
                    LOGGER.error("error while calling spill() on " + consumer, e);
                    throw new RuntimeException(e.getMessage());
                } catch (IOException e) {
                    LOGGER.error("error while calling spill() on " + consumer, e);
                    throw new OutOfMemoryError("error while calling spill() on " + consumer + " : "
                            + e.getMessage());
                }
            }

            consumers.add(consumer);
            LOGGER.debug("Task {} acquired {} for {}", taskAttemptId, bytesToString(got), consumer);
            return got;
        }
    }

    public void releaseExecutionMemory(long required, MemoryConsumer consumer) {
        LOGGER.debug("Task {} release {} from {}", taskAttemptId, bytesToString(required), consumer);
        memoryManager.releaseExecutionMemory(required, taskAttemptId, consumer.getMode());
    }

    /**
     * Dump the memory usage of all consumers.
     */
    public void showMemoryUsage() {
        LOGGER.info("Memory used in task " + taskAttemptId);
        synchronized (this) {
            long memoryAccountedForByConsumers = 0;
            for (MemoryConsumer c: consumers) {
                long totalMemUsage = c.getUsed();
                memoryAccountedForByConsumers += totalMemUsage;
                if (totalMemUsage > 0) {
                    LOGGER.info("Acquired by " + c + ": " + bytesToString(totalMemUsage));
                }
            }
            long memoryNotAccountedFor =
                    memoryManager.getExecutionMemoryUsageForTask(taskAttemptId) - memoryAccountedForByConsumers;
            LOGGER.info(
                    "{} bytes of memory were used by task {} but are not associated with specific consumers",
                    memoryNotAccountedFor, taskAttemptId);
            LOGGER.info(
                    "{} bytes of memory are used for execution and {} bytes of memory are used for storage",
                    memoryManager.executionMemoryUsed(), memoryManager.storageMemoryUsed());
        }
    }

    /**
     * Return the page size in bytes.
     */
    public long pageSizeBytes() {
        return memoryManager.pageSizeBytes();
    }

    /**
     * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
     * intended for allocating large blocks of Tungsten memory that will be shared between operators.
     *
     * Returns `null` if there was not enough memory to allocate the page. May return a page that
     * contains fewer bytes than requested, so callers should verify the size of returned pages.
     */
    public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
        assert(consumer != null);
        assert(consumer.getMode() == tungstenMemoryMode);
        // 申请内存量必须小于内存页
        if (size > MAXIMUM_PAGE_SIZE_BYTES) {
            throw new IllegalArgumentException(
                    "Cannot allocate a page with more than " + MAXIMUM_PAGE_SIZE_BYTES + " bytes");
        }

        long acquired = acquireExecutionMemory(size, consumer);
        if (acquired <= 0) {
            return null;
        }

        final int pageNumber;
        synchronized (this) {
            pageNumber = allocatedPages.nextClearBit(0);
            if (pageNumber >= PAGE_TABLE_SIZE) {
                releaseExecutionMemory(acquired, consumer);
                throw new IllegalStateException(
                        "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
            }
            allocatedPages.set(pageNumber);
        }
        MemoryBlock page = null;
        try {
            page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
        } catch (OutOfMemoryError e) {
            LOGGER.warn("Failed to allocate a page ({} bytes), try again.", acquired);
            // there is no enough memory actually, it means the actual free memory is smaller than
            // MemoryManager thought, we should keep the acquired memory.
            synchronized (this) {
                acquiredButNotUsed += acquired;
                allocatedPages.clear(pageNumber);
            }
            // this could trigger spilling to free some pages.
            return allocatePage(size, consumer);
        }
        page.pageNumber = pageNumber;
        pageTable[pageNumber] = page;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Allocate page number {} ({} bytes)", pageNumber, acquired);
        }
        return page;
    }

    /**
     * Free a block of memory allocated via {@link TaskMemoryManager#allocatePage}.
     */
    public void freePage(MemoryBlock page, MemoryConsumer consumer) {
        assert (page.pageNumber != -1) :
                "Called freePage() on memory that wasn't allocated with allocatePage()";
        assert(allocatedPages.get(page.pageNumber));
        pageTable[page.pageNumber] = null;
        synchronized (this) {
            allocatedPages.clear(page.pageNumber);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Freed page number {} ({} bytes)", page.pageNumber, page.size());
        }
        long pageSize = page.size();
        memoryManager.tungstenMemoryAllocator().free(page);
        releaseExecutionMemory(pageSize, consumer);
    }

    /**
     * Given a memory page and offset within that page, encode this address into a 64-bit long.
     * This address will remain valid as long as the corresponding page has not been freed.
     *
     * @param page a data page allocated by {@link TaskMemoryManager#allocatePage}/
     * @param offsetInPage an offset in this page which incorporates the base offset. In other words,
     *                     this should be the value that you would pass as the base offset into an
     *                     UNSAFE call (e.g. page.baseOffset() + something).
     * @return an encoded page address.
     */
    public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
        if (tungstenMemoryMode == MemoryMode.OFF_HEAP) {
            // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
            // encode. Due to our page size limitation, though, we can convert this into an offset that's
            // relative to the page's base offset; this relative offset will fit in 51 bits.
            offsetInPage -= page.getBaseOffset();
        }
        return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
    }

    @VisibleForTesting
    public static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
        assert (pageNumber != -1) : "encodePageNumberAndOffset called with invalid page";
        return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
    }

    @VisibleForTesting
    public static int decodePageNumber(long pagePlusOffsetAddress) {
        return (int) (pagePlusOffsetAddress >>> OFFSET_BITS);
    }

    private static long decodeOffset(long pagePlusOffsetAddress) {
        return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
    }

    /**
     * Get the page associated with an address encoded by
     * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
     */
    public Object getPage(long pagePlusOffsetAddress) {
        if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
            final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
            assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
            final MemoryBlock page = pageTable[pageNumber];
            assert (page != null);
            assert (page.getBaseObject() != null);
            return page.getBaseObject();
        } else {
            return null;
        }
    }

    /**
     * Get the offset associated with an address encoded by
     * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
     */
    public long getOffsetInPage(long pagePlusOffsetAddress) {
        final long offsetInPage = decodeOffset(pagePlusOffsetAddress);
        if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
            return offsetInPage;
        } else {
            // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
            // converted the absolute address into a relative address. Here, we invert that operation:
            final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
            assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
            final MemoryBlock page = pageTable[pageNumber];
            assert (page != null);
            return page.getBaseOffset() + offsetInPage;
        }
    }

    /**
     * Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return
     * value can be used to detect memory leaks.
     */
    public long cleanUpAllAllocatedMemory() {
        synchronized (this) {
            consumers.forEach(c -> {
                if (c != null && c.getUsed() > 0) {
                    // In case of failed task, it's normal to see leaked memory
                    LOGGER.debug("unreleased " + bytesToString(c.getUsed()) + " memory from " + c);
                }
            });
            consumers.clear();

            for (MemoryBlock page : pageTable) {
                if (page != null) {
                    LOGGER.debug("unreleased page: " + page + " in task " + taskAttemptId);
                    memoryManager.tungstenMemoryAllocator().free(page);
                }
            }
            Arrays.fill(pageTable, null);
        }

        // release the memory that is not used by any consumer (acquired for pages in tungsten mode).
        memoryManager.releaseExecutionMemory(acquiredButNotUsed, taskAttemptId, tungstenMemoryMode);

        return memoryManager.releaseAllExecutionMemoryForTask(taskAttemptId);
    }

    /**
     * Returns the memory consumption, in bytes, for the current task.
     */
    public long getMemoryConsumptionForThisTask() {
        return memoryManager.getExecutionMemoryUsageForTask(taskAttemptId);
    }

    /**
     * Returns Tungsten memory mode
     */
    public MemoryMode getTungstenMemoryMode() {
        return tungstenMemoryMode;
    }
}
