package com.sdu.spark.utils.colleciton;

/**
 * @author hanhan.zhang
 * */
public abstract class SortDataFormat<K, Buffer> {

    public K newKey() {
        return null;
    }

    /**
     * Return the sort key for the element at the given index.
     * */
    public abstract K getKey(Buffer data, int pos);

    /**
     * Returns the sort key for the element at the given index and reuse the input key if possible.
     * The default implementation ignores the reuse parameter and invokes {@link #getKey(Object, int)}.
     * If you want to override this method, you must implement {@link #newKey()}.
     */
    public K getKey(Buffer data, int pos, K reuse) {
        return getKey(data, pos);
    }

    /**
     * Swap two elements.
     * */
    public abstract void swap(Buffer data, int pos0, int pos1);

    /**
     * Copy a single element from src(srcPos) to dst(dstPos).
     * */
    public abstract void copyElement(Buffer src, int srcPos, Buffer dst, int dstPos);

    /**
     * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
     * Overlapping ranges are allowed.
     */
    public abstract void copyRange(Buffer src, int srcPos, Buffer dst, int dstPos, int length);

    /**
     * Allocates a Buffer that can hold up to 'length' elements.
     * All elements of the buffer should be considered invalid until data is explicitly copied in.
     */
    public abstract Buffer allocate(int length, Class<?> cls);

}
