/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Interface representing an immutable bitmap.
 *
 */
public interface ImmutableBitmapDataProvider {
    /**
     * @return a custom iterator over set bits, the bits are traversed
     * in ascending sorted order
     */
    public IntIterator getIntIterator();

    /**
     * Report the number of bytes required to serialize this bitmap.
     * This is the number of bytes written out when using the serialize
     * method. When using the writeExternal method, the count will be
     * higher due to the overhead of Java serialization.
     *
     * @return the size in bytes
     */
    public int serializedSizeInBytes();

    /**
     * Serialize this bitmap.
     *
     * The current bitmap is not modified.
     *
     * @param out the DataOutput stream
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void serialize(DataOutput out) throws IOException;

    /**
     * Checks whether the value in included, which is equivalent to checking
     * if the corresponding bit is set (get in BitSet class).
     *
     * @param x integer value
     * @return whether the integer value is included.
     */
    public boolean contains(int x);

    /**
     * Returns the number of distinct integers added to the bitmap (e.g.,
     * number of bits set).
     *
     * @return the cardinality
     */
    public int getCardinality();

    /**
     * @return a custom iterator over set bits, the bits are traversed
     * in descending sorted order
     */
    public IntIterator getReverseIntIterator();

    /**
     * Estimate of the memory usage of this data structure.
     *
     * @return estimated memory usage.
     */
    public int getSizeInBytes();

    /**
     * Checks whether the bitmap is empty.
     *
     * @return true if this bitmap contains no set bit
     */
    public boolean isEmpty();

    /**
     * Return the set values as an array. The integer
     * values are in sorted order.
     *
     * @return array representing the set values.
     */
    public int[] toArray();
    
    /**
     * Return the jth value stored in this bitmap.
     *
     * @param j index of the value
     *
     * @return the value
     */
    public int select(int j);

    /**
     * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be GetCardinality()).
     * @param x upper limit
     *
     * @return the rank
     */
    public int rank(int x);

    /**
     * Create a new bitmap of the same class, containing at most maxcardinality integers.
     *
     * @param x maximal cardinality
     * @return a new bitmap with cardinality no more than maxcardinality
     */
    public ImmutableBitmapDataProvider limit (int x);

}
