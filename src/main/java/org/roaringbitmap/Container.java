/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;

/**
 * Base container class.
 */
public abstract class Container implements Iterable<Short>, Cloneable, Externalizable {

    /**
     * Create a container initialized with a range of consecutive values
     *
     * @param start first index
     * @param last  last index (range in inclusive)
     * @return a new container initialized with the specified values
     */
    public static Container rangeOfOnes(final int start, final int last) {
        if (last - start + 1 > ArrayContainer.DEFAULT_MAX_SIZE)
            return new BitmapContainer(start, last);
        return new ArrayContainer(start, last);
    }

    /**
     * Add a short to the container. May generate a new container.
     *
     * @param x short to be added
     * @return the new container
     */
    public abstract Container add(short x);

    /**
     * Computes the bitwise AND of this container with another
     * (intersection). This container as well as the provided container are
     * left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container and(ArrayContainer x);

    /**
     * Computes the bitwise AND of this container with another
     * (intersection). This container as well as the provided container are
     * left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container and(BitmapContainer x);

    /**
     * Computes the bitwise AND of this container with another
     * (intersection). This container as well as the provided container are
     * left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public Container and(Container x) {
        if (x instanceof ArrayContainer)
            return and((ArrayContainer) x);
        return and((BitmapContainer) x);

    }

    /**
     * Computes the bitwise ANDNOT of this container with another
     * (difference). This container as well as the provided container are
     * left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container andNot(ArrayContainer x);

    /**
     * Computes the bitwise ANDNOT of this container with another
     * (difference). This container as well as the provided container are
     * left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container andNot(BitmapContainer x);

    /**
     * Computes the bitwise ANDNOT of this container with another
     * (difference). This container as well as the provided container are
     * left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public Container andNot(Container x) {
        if (x instanceof ArrayContainer)
            return andNot((ArrayContainer) x);
        return andNot((BitmapContainer) x);
    }

    /**
     * Empties the container
     */
    public abstract void clear();

    @Override
    public abstract Container clone();

    /**
     * Checks whether the contain contains the provided value
     *
     * @param x value to check
     * @return whether the value is in the container
     */
    public abstract boolean contains(short x);

    /**
     * Deserialize (recover) the container.
     *
     * @param in the DataInput stream
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void deserialize(DataInput in) throws IOException;

    /**
     * Fill the least significant 16 bits of the integer array, starting at
     * index index, with the short values from this container. The caller is
     * responsible to allocate enough room. The most significant 16 bits of
     * each integer are given by the most significant bits of the provided
     * mask.
     *
     * @param x    provided array
     * @param i    starting index
     * @param mask indicates most significant bits
     */
    public abstract void fillLeastSignificant16bits(int[] x, int i, int mask);

    /**
     * Size of the underlying array
     *
     * @return size in bytes
     */
    protected abstract int getArraySizeInBytes();

    /**
     * Computes the distinct number of short values in the container. Can be
     * expected to run in constant time.
     *
     * @return the cardinality
     */
    public abstract int getCardinality();

    /**
     * Iterator to visit the short values in the container
     *
     * @return iterator
     */
    public abstract ShortIterator getShortIterator();

    /**
     * Computes an estimate of the memory usage of this container. The
     * estimate is not meant to be exact.
     *
     * @return estimated memory usage in bytes
     */
    public abstract int getSizeInBytes();

    /**
     * Computes the in-place bitwise AND of this container with another
     * (intersection). The current container is generally modified, whereas
     * the provided container (x) is unaffected. May generate a new
     * container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container iand(ArrayContainer x);

    /**
     * Computes the in-place bitwise AND of this container with another
     * (intersection). The current container is generally modified, whereas
     * the provided container (x) is unaffected. May generate a new
     * container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container iand(BitmapContainer x);

    /**
     * Computes the in-place bitwise AND of this container with another
     * (intersection). The current container is generally modified, whereas
     * the provided container (x) is unaffected. May generate a new
     * container.
     *
     * @param x other container
     * @return aggregated container
     */
    public Container iand(Container x) {
        if (x instanceof ArrayContainer)
            return iand((ArrayContainer) x);
        else
            return iand((BitmapContainer) x);
    }

    /**
     * Computes the in-place bitwise ANDNOT of this container with another
     * (difference). The current container is generally modified, whereas
     * the provided container (x) is unaffected. May generate a new
     * container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container iandNot(ArrayContainer x);

    /**
     * Computes the in-place bitwise ANDNOT of this container with another
     * (difference). The current container is generally modified, whereas
     * the provided container (x) is unaffected. May generate a new
     * container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container iandNot(BitmapContainer x);

    /**
     * Computes the in-place bitwise ANDNOT of this container with another
     * (difference). The current container is generally modified, whereas
     * the provided container (x) is unaffected. May generate a new
     * container.
     *
     * @param x other container
     * @return aggregated container
     */
    public Container iandNot(Container x) {
        if (x instanceof ArrayContainer)
            return iandNot((ArrayContainer) x);
        return iandNot((BitmapContainer) x);
    }

    /**
     * Computes the in-place bitwise NOT of this container (complement).
     * Only those bits within the range are affected. The current container
     * is generally modified. May generate a new container.
     *
     * @param rangeStart beginning of range (inclusive); 0 is beginning of this
     *                   container.
     * @param rangeEnd   ending of range (exclusive)
     * @return (partially) completmented container
     */
    public abstract Container inot(int rangeStart, int rangeEnd);

    /**
     * Computes the in-place bitwise OR of this container with another
     * (union). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container ior(ArrayContainer x);

    /**
     * Computes the in-place bitwise OR of this container with another
     * (union). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container ior(BitmapContainer x);

    /**
     * Computes the in-place bitwise OR of this container with another
     * (union). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     *
     * @param x other container
     * @return aggregated container
     */
    public Container ior(Container x) {
        if (x instanceof ArrayContainer)
            return ior((ArrayContainer) x);
        return ior((BitmapContainer) x);
    }

    /**
     * Computes the in-place bitwise OR of this container with another
     * (union). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container ixor(ArrayContainer x);

    /**
     * Computes the in-place bitwise OR of this container with another
     * (union). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container ixor(BitmapContainer x);

    /**
     * Computes the in-place bitwise OR of this container with another
     * (union). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     *
     * @param x other container
     * @return aggregated container
     */
    public Container ixor(Container x) {
        if (x instanceof ArrayContainer)
            return ixor((ArrayContainer) x);
        return ixor((BitmapContainer) x);
    }

    protected Container lazyOR(Container x) {
        if (this instanceof ArrayContainer) {
            if (x instanceof ArrayContainer)
                return or((ArrayContainer) x);
            return or((BitmapContainer) x);
        } else {
            if (x instanceof ArrayContainer)
                return ((BitmapContainer)this).lazyor((ArrayContainer) x);
            return ((BitmapContainer)this).lazyor((BitmapContainer) x);
        }
    }
    
    protected Container lazyIOR(Container x) {
        if (this instanceof ArrayContainer) {
            if (x instanceof ArrayContainer)
                return ior((ArrayContainer) x);
            return ior((BitmapContainer) x);
        } else {
            if (x instanceof ArrayContainer)
                return ((BitmapContainer)this).ilazyor((ArrayContainer) x);
            return ((BitmapContainer)this).ilazyor((BitmapContainer) x);
        }
    }
    
    /**
     * Computes the bitwise NOT of this container (complement). Only those
     * bits within the range are affected. The current container is left
     * unaffected.
     *
     * @param rangeStart beginning of range (inclusive); 0 is beginning of this
     *                   container.
     * @param rangeEnd   ending of range (exclusive)
     * @return (partially) completmented container
     */
    public abstract Container not(int rangeStart, int rangeEnd);

    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container or(ArrayContainer x);

    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container or(BitmapContainer x);

    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public Container or(Container x) {
        if (x instanceof ArrayContainer)
            return or((ArrayContainer) x);
        return or((BitmapContainer) x);
    }

    /**
     * Remove the short from this container. May create a new container.
     *
     * @param x to be removed
     * @return New container
     */
    public abstract Container remove(short x);

    /**
     * Serialize the container.
     *
     * @param out the DataOutput stream
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void serialize(DataOutput out) throws IOException;

    /**
     * Report the number of bytes required to serialize this container.
     *
     * @return the size in bytes
     */
    public abstract int serializedSizeInBytes();

    /**
     * If possible, recover wasted memory.
     */
    public abstract void trim();

    /**
     * Write just the underlying array.
     *
     * @param out output stream
     * @throws IOException in case of failure
     */
    protected abstract void writeArray(DataOutput out) throws IOException;

    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container xor(ArrayContainer x);

    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container xor(BitmapContainer x);

    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other parameter
     * @return aggregated container
     */
    public Container xor(Container x) {
        if (x instanceof ArrayContainer)
            return xor((ArrayContainer) x);
        return xor((BitmapContainer) x);
    }
}
