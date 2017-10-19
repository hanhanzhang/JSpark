package com.sdu.spark.utils.colleciton;

import com.sdu.spark.SparkEnv;
import com.sdu.spark.memory.MemoryConsumer;
import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 *
 * @author hanhan.zhang
 * */
public abstract class Spillable<C> extends MemoryConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Spillable.class);

    protected TaskMemoryManager taskMemoryManager;

    // Initial threshold for the size of a collection before we start tracking its memory usage
    // For testing only
    private long initialMemoryThreshold;
    // Force this collection to spill when there are this many elements in memory
    // For testing only
    private long numElementsForceSpillThreshold;

    private volatile long myMemoryThreshold;

    // Number of elements read from input since last spill
    protected long elementsRead = 0L;
    // Number of bytes spilled in total
    private volatile long memoryBytesSpilled = 0L;
    // Number of spills
    private int spillCount = 0;

    public Spillable(TaskMemoryManager taskMemoryManager) {
        super(taskMemoryManager);
        this.taskMemoryManager = taskMemoryManager;

        this.initialMemoryThreshold = SparkEnv.env.conf.getLong("spark.shuffle.spill.initialMemoryThreshold",
                                                                5 * 1024 * 1024);
        this.numElementsForceSpillThreshold = SparkEnv.env.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
                                                                        Long.MAX_VALUE);
        this.myMemoryThreshold = initialMemoryThreshold;
    }

    public long elementsRead() {
        return elementsRead;
    }

    public void addElementsRead() {
        elementsRead += 1;
    }

    /**
     * Spills the current in-memory collection to disk if needed. Attempts to acquire more
     * memory before spilling.
     *
     * @param collection collection to spill to disk
     * @param currentMemory collection占用内存估量
     * @return true if `collection` was spilled to disk; false otherwise
     */
    protected boolean maybeSpill(C collection, long currentMemory) {
        boolean shouldSpill = false;
        /**{@link TimeSort#MIN_MERGE}*/
        if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
            long amountToRequest = 2 * currentMemory - myMemoryThreshold;
            long granted = acquireMemory(amountToRequest);
            myMemoryThreshold += granted;
            shouldSpill = currentMemory >= myMemoryThreshold;
        }

        shouldSpill = shouldSpill || elementsRead > numElementsForceSpillThreshold;
        if (shouldSpill) {
            spillCount += 1;
            logSpillage(currentMemory);
            spill(collection);
            elementsRead = 0;
            memoryBytesSpilled += currentMemory;
            releaseMemory();
        }
        return shouldSpill;
    }

    /**
     * Release our memory back to the execution pool so that other tasks can grab it.
     */
    protected void releaseMemory() {
        freeMemory(myMemoryThreshold - initialMemoryThreshold);
        myMemoryThreshold = initialMemoryThreshold;
    }

    private void logSpillage(long size) {
        long threadId = Thread.currentThread().getId();
        LOGGER.info("Thread {} spilling in-memory map of {} to disk ({} time{} so far)",
                threadId, Utils.bytesToString(size), spillCount, spillCount > 1 ? "s" : "");
    }

    public abstract boolean forceSpill();

    public abstract void spill(C collection);
}

