/*
 * (c) the authors
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
     * Get the name of this container. 
     * 
     * @return name of the container
     */
    public String getContainerName() {
        if (this instanceof BitmapContainer) {
            return "bitmap ";
        } else if (this instanceof ArrayContainer) {
            return "array";
        } else {
            return "run";
        }
    }
    
    /**
     * Create a container initialized with a range of consecutive values
     *
     * @param start first index
     * @param last  last index (range is exclusive)
     * @return a new container initialized with the specified values
     */
    public static Container rangeOfOnes(final int start, final int last) {
        Container answer = new RunContainer();
        answer = answer.iadd(start, last);
        return answer;
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
    public abstract Container and(RunContainer x);


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
        else if (x instanceof BitmapContainer)
            return and((BitmapContainer) x);
        return and((RunContainer) x);
    }

    /**
     * Computes the bitwise AND of this container with another
     * (intersection). This container as well as the provided container are
     * left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public int andCardinality(Container x) {
        if (this.getCardinality() == 0)
            return 0;
        else if (x.getCardinality() == 0)
            return 0;
        else {
            if (x instanceof ArrayContainer)
                return andCardinality((ArrayContainer) x);
            else if (x instanceof BitmapContainer)
                return andCardinality((BitmapContainer) x);
            return andCardinality((RunContainer) x);
        }
    }
    
    public abstract int andCardinality(ArrayContainer x);
    public abstract int andCardinality(BitmapContainer x);
    public abstract int andCardinality(RunContainer x);
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
    public abstract Container andNot(RunContainer x);


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
        else if (x instanceof BitmapContainer)
            return andNot((BitmapContainer) x);
        return andNot((RunContainer) x);
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
     * index i, with the short values from this container. The caller is
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
     * Add a short to the container if it is not present, otherwise remove it. 
     * May generate a new container.
     *
     * @param x short to be added
     * @return the new container
     */
    public abstract Container flip(short x);
    
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
    public abstract Container iand(RunContainer x);


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
        else if (x instanceof BitmapContainer)
            return iand((BitmapContainer) x);
        return iand((RunContainer) x);
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
    public abstract Container iandNot(RunContainer x);


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
        else if (x instanceof BitmapContainer)
            return iandNot((BitmapContainer) x);
        return iandNot((RunContainer) x);
    }

    /**
     * Computes the in-place bitwise NOT of this container (complement).
     * Only those bits within the range are affected. The current container
     * is generally modified. May generate a new container.
     *
     * @param rangeStart beginning of range (inclusive); 0 is beginning of this
     *                   container.
     * @param rangeEnd   ending of range (exclusive)
     * @return (partially) complemented container
     */
    public abstract Container inot(int rangeStart, int rangeEnd);

    /**
     * Returns true if the current container intersects the other container.
     *
     * @param x other container
     * @return whether they intersect
     */
    public boolean intersects(Container x) {
        if (x instanceof ArrayContainer)
            return intersects((ArrayContainer) x);
        else if (x instanceof BitmapContainer)
            return intersects((BitmapContainer) x);
        return intersects((RunContainer) x);
    }

    
    /**
     * Returns true if the current container intersects the other container.
     *
     * @param x other container
     * @return whether they intersect
      */
    public abstract boolean intersects(ArrayContainer x);

    /**
     * Returns true if the current container intersects the other container.
     *
     * @param x other container
     * @return whether they intersect
      */
    public abstract boolean intersects(BitmapContainer x);

    /**
     * Returns true if the current container intersects the other container.
     *
     * @param x other container
     * @return whether they intersect
      */
    public abstract boolean intersects(RunContainer x);
    
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
    public abstract Container ior(RunContainer x);

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
        else if (x instanceof BitmapContainer)
            return ior((BitmapContainer) x);
        return ior((RunContainer) x);
    }

    /**
     * Computes the in-place bitwise XOR of this container with another
     * (symmetric difference). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container ixor(ArrayContainer x);

    /**
     * Computes the in-place bitwise XOR of this container with another
     * (symmetric difference). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container ixor(BitmapContainer x);

    /**
     * Computes the in-place bitwise XOR of this container with another
     * (symmetric difference). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container ixor(RunContainer x);

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
        else if (x instanceof BitmapContainer)
            return ixor((BitmapContainer) x);
        return ixor((RunContainer) x);
    }


    /**
     * Computes the bitwise OR of this container with another (union). This
     * container as well as the provided container are left unaffected.
     * The resulting container may not track its cardinality correctly. This
     * can be fixed as follows:   if(c.getCardinality()&lt;0) ((BitmapContainer)c).computeCardinality();
     *    
     * @param x other container
     * @return aggregated container
     */
    public Container lazyOR(Container x) {
        if (this instanceof ArrayContainer) {
            if (x instanceof ArrayContainer)
                return or((ArrayContainer) x);
            else if (x instanceof BitmapContainer) 
                return ((BitmapContainer)x).lazyor((ArrayContainer) this);
            return ((RunContainer) x).lazyor((ArrayContainer) this);
        } else if (this instanceof RunContainer) {
            if (x instanceof ArrayContainer)
                return ((RunContainer)this).lazyor((ArrayContainer) x);
            else if (x instanceof BitmapContainer) 
                return ((BitmapContainer) x).lazyor((RunContainer) this);
            return or((RunContainer) x);
        } else  {
            if (x instanceof ArrayContainer)
                return ((BitmapContainer)this).lazyor((ArrayContainer) x);
            else if (x instanceof BitmapContainer) return ((BitmapContainer)this).lazyor((BitmapContainer) x);
            return ((BitmapContainer)this).lazyor((RunContainer) x);
        }
    }
    /**
     * Computes the in-place bitwise OR of this container with another
     * (union). The current container is generally modified, whereas the
     * provided container (x) is unaffected. May generate a new container.
     * The resulting container may not track its cardinality correctly.
     * The resulting container may not track its cardinality correctly. This
     * can be fixed as follows:   if(c.getCardinality()&lt;0) ((BitmapContainer)c).computeCardinality();
     *
     * @param x other container
     * @return aggregated container
     */
    public Container lazyIOR(Container x) {
        if (this instanceof ArrayContainer) {
            if (x instanceof ArrayContainer)
                return ior((ArrayContainer) x);
            else if (x instanceof BitmapContainer) return ior((BitmapContainer) x);
            return ((RunContainer) x).lazyor((ArrayContainer) this);
        } else if (this instanceof RunContainer) {
            if (x instanceof ArrayContainer)
                return ((RunContainer) this).ilazyor((ArrayContainer) x);
            else if (x instanceof BitmapContainer) return ior((BitmapContainer) x);
            return ior((RunContainer) x);
        } else {
            if (x instanceof ArrayContainer)
                return ((BitmapContainer)this).ilazyor((ArrayContainer) x);
            else if (x instanceof BitmapContainer) return ((BitmapContainer)this).ilazyor((BitmapContainer) x);
            return ((BitmapContainer)this).ilazyor((RunContainer) x);
        }
    }
    
    /**
     * The output of a lazyOR or lazyIOR might be an invalid container, this
     * should be called on it.
     * @return a new valid container
     */
    public abstract Container repairAfterLazy();
    
    /**
     * Computes the bitwise NOT of this container (complement). Only those
     * bits within the range are affected. The current container is left
     * unaffected.
     *
     * @param rangeStart beginning of range (inclusive); 0 is beginning of this
     *                   container.
     * @param rangeEnd   ending of range (exclusive)
     * @return (partially) complemented container
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
    public abstract Container or(RunContainer x);


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
        else if (x instanceof BitmapContainer)
            return or((BitmapContainer) x);
        return or((RunContainer) x);
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
     * Computes the bitwise XOR of this container with another (symmetric difference). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container xor(ArrayContainer x);

    /**
     * Computes the bitwise XOR of this container with another (symmetric difference). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container xor(BitmapContainer x);

    /**
     * Computes the bitwise XOR of this container with another (symmetric difference). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other container
     * @return aggregated container
     */
    public abstract Container xor(RunContainer x);

    /**
     * Computes the bitwise OR of this container with another (symmetric difference). This
     * container as well as the provided container are left unaffected.
     *
     * @param x other parameter
     * @return aggregated container
     */
    public Container xor(Container x) {
        if (x instanceof ArrayContainer)
            return xor((ArrayContainer) x);
        else if (x instanceof BitmapContainer)
            return xor((BitmapContainer) x);
        return xor((RunContainer) x);
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
     * Create a new Container containing at most maxcardinality integers.
     * 
     * @param maxcardinality maximal cardinality
     * @return a new bitmap with cardinality no more than maxcardinality
     */
    public abstract Container limit(int maxcardinality);

    
     /**
      * Add all shorts in [begin,end) using an unsigned interpretation. May generate a new container.
      *
      * @param begin start of range (inclusive)
      * @param end end of range (exclusive)
      * @return the new container
      */
     public abstract Container iadd(int begin, int end);
    
     /**
      * Remove shorts in [begin,end) using an unsigned interpretation. May generate a new container.
      *
      * @param begin start of range (inclusive)
      * @param end end of range (exclusive)
      * @return the new container
      */
     public abstract Container iremove(int begin, int end);


     /**
      * Return a new container with all shorts in [begin,end) 
      * added using an unsigned interpretation. 
      *
      * @param begin start of range (inclusive)
      * @param end end of range (exclusive)
      * @return the new container
      */
     public abstract Container add(int begin, int end);
    
     /**
      * Return a new container with all shorts in [begin,end) 
      * remove using an unsigned interpretation. 
      *
      * @param begin start of range (inclusive)
      * @param end end of range (exclusive)
      * @return the new container
      */
     public abstract Container remove(int begin, int end);


     /**
      * Convert to RunContainers, when the result is smaller.  Overridden by RunContainer
      *   to possibility switch from RunContainer to a smaller alternative.
      *   Overridden by BitmapContainer with a more efficient approach.
      *   @return the new container
      */
     public abstract Container runOptimize();

     abstract int numberOfRuns(); // exact
}
