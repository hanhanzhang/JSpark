package com.sdu.spark.utils.colleciton;

import com.sdu.spark.SparkEnv;
import com.sdu.spark.memory.MemoryConsumer;
import com.sdu.spark.memory.MemoryMode;
import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 *
 * @author hanhan.zhang
 * */
public abstract class Spillable<C> extends MemoryConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Spillable.class);

    // Initial threshold for the size of a collection before we start tracking its memory usage
    // For testing only
    private long initialMemoryThreshold;
    // Force this collection to spill when there are this many elements in memory
    // For testing only
    private long numElementsForceSpillThreshold;

    // Threshold for this collection's size in bytes before we start tracking its memory usage
    // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
    private volatile long myMemoryThreshold;

    // Number of elements read from input since last spill
    protected long elementsRead = 0L;
    // Number of bytes spilled in total
    private volatile long memoryBytesSpilled = 0L;
    // Number of spills
    private int spillCount = 0;

    public Spillable(TaskMemoryManager taskMemoryManager) {
        super(taskMemoryManager);

        this.initialMemoryThreshold = SparkEnv.env.conf.getLong("spark.shuffle.spill.initialMemoryThreshold", 5 * 1024 * 1024);
        this.numElementsForceSpillThreshold = SparkEnv.env.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold", Long.MAX_VALUE);
        this.myMemoryThreshold = initialMemoryThreshold;
    }

    protected void addElementsRead() {
        elementsRead += 1;
    }

    /**
     * Spills the current in-memory collection to disk if needed. Attempts to acquire more
     * memory before spilling.
     *
     * @param collection collection to spill to disk
     * @param currentMemory  estimated size of the collection in bytes
     * @return true if `collection` was spilled to disk; false otherwise
     */
    protected boolean maybeSpill(C collection, long currentMemory) {
        boolean shouldSpill = false;
        if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
            // Claim up to double our current memory from the shuffle memory pool
            long amountToRequest = 2 * currentMemory - myMemoryThreshold;
            long granted = acquireMemory(amountToRequest);
            myMemoryThreshold += granted;
            // If we were granted too little memory to grow further (either tryToAcquire returned 0,
            // or we already had more memory than myMemoryThreshold), spill the current collection
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

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        if (trigger != this && taskMemoryManager.getTungstenMemoryMode() == MemoryMode.ON_HEAP) {
            boolean isSpilled = forceSpill();
            if (!isSpilled) {
                return 0L;
            }
            long freeMemory = myMemoryThreshold - initialMemoryThreshold;
            memoryBytesSpilled += freeMemory;
            releaseMemory();
            return freeMemory;
        }
        return 0L;
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

    /**
     * Force to spilling the current in-memory collection to disk to release memory,
     * It will be called by TaskMemoryManager when there is not enough memory for the task.
     * */
    public abstract boolean forceSpill();

    /**
     * Spills the current in-memory collection to disk, and releases the memory.
     * */
    public abstract void spill(C collection);
}

