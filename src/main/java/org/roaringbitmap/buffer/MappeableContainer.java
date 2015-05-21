/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.ShortIterator;

import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;

/**
 * Base container class. This class is similar to org.roaringbitmap.Container
 * but meant to be used with memory mapping.
 */
public abstract class MappeableContainer implements Iterable<Short>, Cloneable,
        Externalizable {

    /**
     * Create a container initialized with a range of consecutive values
     * 
     * @param start
     *            first index
     * @param last
     *            last index (range is exclusive)
     * @return a new container initialized with the specified values
     */
    public static MappeableContainer rangeOfOnes(final int start, final int last) {
        if (last - start  > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return new MappeableBitmapContainer(start, last);
        return new MappeableArrayContainer(start, last);
    }

    /**
     * Add a short to the container. May generate a new container.
     * 
     * @param x
     *            short to be added
     * @return the new container
     */
    public abstract MappeableContainer add(short x);

    /**
     * Computes the bitwise AND of this container with another (intersection).
     * This container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer and(MappeableArrayContainer x);

    /**
     * Computes the bitwise AND of this container with another (intersection).
     * This container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer and(MappeableBitmapContainer x);

    /**
     * Computes the bitwise AND of this container with another (intersection).
     * This container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public MappeableContainer and(MappeableContainer x) {
        if (x instanceof MappeableArrayContainer)
            return and((MappeableArrayContainer) x);
        return and((MappeableBitmapContainer) x);

    }

    /**
     * Computes the bitwise ANDNOT of this container with another (difference).
     * This container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer andNot(MappeableArrayContainer x);

    /**
     * Computes the bitwise ANDNOT of this container with another (difference).
     * This container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer andNot(MappeableBitmapContainer x);

    /**
     * Computes the bitwise ANDNOT of this container with another (difference).
     * This container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public MappeableContainer andNot(MappeableContainer x) {
        if (x instanceof MappeableArrayContainer)
            return andNot((MappeableArrayContainer) x);
        return andNot((MappeableBitmapContainer) x);
    }

    /**
     * Empties the container
     */
    public abstract void clear();

    @Override
    public abstract MappeableContainer clone();

    /**
     * Checks whether the contain contains the provided value
     * 
     * @param x
     *            value to check
     * @return whether the value is in the container
     */
    public abstract boolean contains(short x);

    /**
     * Fill the least significant 16 bits of the integer array, starting at
     * index index, with the short values from this container. The caller is
     * responsible to allocate enough room. The most significant 16 bits of each
     * integer are given by the most significant bits of the provided mask.
     * 
     * @param x
     *            provided array
     * @param i
     *            starting index
     * @param mask
     *            indicates most significant bits
     */
    public abstract void fillLeastSignificant16bits(int[] x, int i, int mask);


    /**
     * Add a short to the container if it is not present, otherwise remove it. 
     * May generate a new container.
     *
     * @param x short to be added
     * @return the new container
     */
    public abstract MappeableContainer flip(short x);
    
    
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
     * Iterator to visit the short values in the container in ascending order.
     *
     * @return iterator
     */
    public abstract ShortIterator getShortIterator();


    /**
     * Iterator to visit the short values in the container in descending order.
     *
     * @return iterator
     */
    public abstract ShortIterator getReverseShortIterator();

    /**
     * Computes an estimate of the memory usage of this container. The estimate
     * is not meant to be exact.
     * 
     * @return estimated memory usage in bytes
     */
    public abstract int getSizeInBytes();

    /**
     * Computes the in-place bitwise AND of this container with another
     * (intersection). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer iand(MappeableArrayContainer x);

    /**
     * Computes the in-place bitwise AND of this container with another
     * (intersection). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer iand(MappeableBitmapContainer x);

    /**
     * Computes the in-place bitwise AND of this container with another
     * (intersection). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public MappeableContainer iand(MappeableContainer x) {
        if (x instanceof MappeableArrayContainer)
            return iand((MappeableArrayContainer) x);
        return iand((MappeableBitmapContainer) x);

    }

    /**
     * Computes the in-place bitwise ANDNOT of this container with another
     * (difference). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer iandNot(MappeableArrayContainer x);

    /**
     * Computes the in-place bitwise ANDNOT of this container with another
     * (difference). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer iandNot(MappeableBitmapContainer x);

    /**
     * Computes the in-place bitwise ANDNOT of this container with another
     * (difference). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public MappeableContainer iandNot(MappeableContainer x) {
        if (x instanceof MappeableArrayContainer)
            return iandNot((MappeableArrayContainer) x);
        return iandNot((MappeableBitmapContainer) x);
    }

    /**
     * Computes the in-place bitwise NOT of this container (complement). Only
     * those bits within the range are affected. The current container is
     * generally modified. May generate a new container.
     * 
     * @param rangeStart
     *            beginning of range (inclusive); 0 is beginning of this
     *            container.
     * @param rangeEnd
     *            ending of range (exclusive)
     * @return (partially) completmented container
     */
    public abstract MappeableContainer inot(int rangeStart, int rangeEnd);

    /**
     * Computes the in-place bitwise OR of this container with another (union).
     * The current container is generally modified, whereas the provided
     * container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer ior(MappeableArrayContainer x);

    /**
     * Computes the in-place bitwise OR of this container with another (union).
     * The current container is generally modified, whereas the provided
     * container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer ior(MappeableBitmapContainer x);

    /**
     * Computes the in-place bitwise OR of this container with another (union).
     * The current container is generally modified, whereas the provided
     * container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public MappeableContainer ior(MappeableContainer x) {
        if (x instanceof MappeableArrayContainer)
            return ior((MappeableArrayContainer) x);
        return ior((MappeableBitmapContainer) x);
    }

    /**
     * Computes the in-place bitwise XOR of this container with another (symmetric difference).
     * The current container is generally modified, whereas the provided
     * container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer ixor(MappeableArrayContainer x);

    /**
     * Computes the in-place bitwise XOR of this container with another (symmetric difference).
     * The current container is generally modified, whereas the provided
     * container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer ixor(MappeableBitmapContainer x);

    /**
     * Computes the in-place bitwise XOR of this container with another (symmetric difference).
     * The current container is generally modified, whereas the provided
     * container (x) is unaffected. May generate a new container.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public MappeableContainer ixor(MappeableContainer x) {
        if (x instanceof MappeableArrayContainer)
            return ixor((MappeableArrayContainer) x);
        else
            return ixor((MappeableBitmapContainer) x);

    }

    protected MappeableContainer lazyOR(MappeableContainer x) {
        if (this instanceof MappeableArrayContainer) {
            if (x instanceof MappeableArrayContainer)
                return or((MappeableArrayContainer) x);
            return or((MappeableBitmapContainer) x);
        } else {
            if (x instanceof MappeableArrayContainer)
                return ((MappeableBitmapContainer)this).lazyor((MappeableArrayContainer) x);
            return ((MappeableBitmapContainer)this).lazyor((MappeableBitmapContainer) x);
        }
    }
    
    protected MappeableContainer lazyIOR(MappeableContainer x) {
        if (this instanceof MappeableArrayContainer) {
            if (x instanceof MappeableArrayContainer)
                return ior((MappeableArrayContainer) x);
            return ior((MappeableBitmapContainer) x);
        } else {
            if (x instanceof MappeableArrayContainer)
                return ((MappeableBitmapContainer)this).ilazyor((MappeableArrayContainer) x);
            return ((MappeableBitmapContainer)this).ilazyor((MappeableBitmapContainer) x);
        }
    }
    
    /**
     * Computes the bitwise NOT of this container (complement). Only those bits
     * within the range are affected. The current container is left unaffected.
     * 
     * @param rangeStart
     *            beginning of range (inclusive); 0 is beginning of this
     *            container.
     * @param rangeEnd
     *            ending of range (exclusive)
     * @return (partially) completmented container
     */
    public abstract MappeableContainer not(int rangeStart, int rangeEnd);

    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer or(MappeableArrayContainer x);

    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer or(MappeableBitmapContainer x);

    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public MappeableContainer or(MappeableContainer x) {
        if (x instanceof MappeableArrayContainer)
            return or((MappeableArrayContainer) x);
        return or((MappeableBitmapContainer) x);
    }

    /**
     * Remove the short from this container. May create a new container.
     * 
     * @param x
     *            to be removed
     * @return New container
     */
    public abstract MappeableContainer remove(short x);

    /**
     * If possible, recover wasted memory.
     */
    public abstract void trim();

    /**
     * Write just the underlying array.
     * 
     * @param out
     *            output stream
     * @throws IOException
     *             in case of failure
     */
    protected abstract void writeArray(DataOutput out) throws IOException;

    /**
     * Computes the bitwise XOR of this container with another (symmetric difference). This
     * container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer xor(MappeableArrayContainer x);

    /**
     * Computes the bitwise XOR of this container with another (symmetric difference). This
     * container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other container
     * @return aggregated container
     */
    public abstract MappeableContainer xor(MappeableBitmapContainer x);

    /**
     * Computes the bitwise XOR of this container with another (symmetric difference). This
     * container as well as the provided container are left unaffected.
     * 
     * @param x
     *            other parameter
     * @return aggregated container
     */
    public MappeableContainer xor(MappeableContainer x) {
        if (x instanceof MappeableArrayContainer)
            return xor((MappeableArrayContainer) x);
        return xor((MappeableBitmapContainer) x);

    }
    
    /**
     * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be GetCardinality()).
     * @param lowbits upper limit
     *
     * @return the rank
     */
    public abstract int rank(short lowbits);


    /**
     * Return the jth value 
     * 
     * @param j index of the value 
     *
     * @return the value
     */
    public abstract short select(int j);


    /**
     * Create a new MappeableContainer containing at most maxcardinality integers.
     * 
     * @param maxcardinality maximal cardinality
     * @return a new bitmap with cardinality no more than maxcardinality
     */
    public abstract MappeableContainer limit(int maxcardinality);

    

    /**
     * Add all shorts in [begin,end) using an unsigned interpretation. May generate a new container.
     *
     * @param begin start of range (inclusive)
     * @param end end of range (exclusive)
     * @return the new container
     */
    public abstract MappeableContainer iadd(int begin, int end);
   
    /**
     * Remove shorts in [begin,end) using an unsigned interpretation. May generate a new container.
     *
     * @param begin start of range (inclusive)
     * @param end end of range (exclusive)
     * @return the new container
     */
    public abstract MappeableContainer iremove(int begin, int end);
    


    /**
     * Return a new container with all shorts in [begin,end) 
     * added using an unsigned interpretation. 
     *
     * @param begin start of range (inclusive)
     * @param end end of range (exclusive)
     * @return the new container
     */
    public abstract MappeableContainer add(int begin, int end);
   
    /**
     * Return a new container with all shorts in [begin,end) 
     * remove using an unsigned interpretation. 
     *
     * @param begin start of range (inclusive)
     * @param end end of range (exclusive)
     * @return the new container
     */
    public abstract MappeableContainer remove(int begin, int end);
}
