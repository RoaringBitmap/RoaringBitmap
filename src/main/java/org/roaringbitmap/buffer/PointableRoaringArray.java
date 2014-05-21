package org.roaringbitmap.buffer;

/**
 * Generic interface for the array underlying roaring bitmap classes.
 * 
 */
public interface PointableRoaringArray extends Cloneable {
    /**
     * @return a copy
     */
    PointableRoaringArray clone();

    /**
     * 
     * 
     * @param x
     *            16-bit key
     * @return matching container
     */
    MappeableContainer getContainer(short x);

    /**
     * @param i
     *            index
     * @return matching container
     */
    MappeableContainer getContainerAtIndex(int i);

    /**
     * @return a ContainerPointer to iterator over the array
     */
    MappeableContainerPointer getContainerPointer();

    /**
     * @param x
     *            16-bit key
     * @return corresponding index
     */
    int getIndex(short x);

    /**
     * @param i
     *            the index
     * @return 16-bit key at the index
     */
    short getKeyAtIndex(int i);

    /**
     * @return number of keys
     */
    int size();
}
