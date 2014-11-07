package org.roaringbitmap.buffer;

/**
 * 
 * This interface allows you to iterate over the containers in a roaring bitmap.
 * 
 */
interface MappeableContainerPointer extends
        Comparable<MappeableContainerPointer>, Cloneable {
    /**
     * Move to the next container
     */
    void advance();

    /**
     * Move to the previous container
     */
    void previous();

    /**
     * Returns the cardinality of the current container. Can be faster than
     * loading the container first.
     * 
     * @return cardinality of the current container
     */
    int getCardinality();

    /**
     * This method can be used to check whether there is current a valid
     * container as it returns null when there is not.
     * 
     * @return null or the current container
     */
    MappeableContainer getContainer();

    /**
     * 
     * @return whether there is a container at the current position
     */
    boolean hasContainer();

    /**
     * The key is a 16-bit integer that indicates the position of the container
     * in the roaring bitmap. To be interpreted as an unsigned integer.
     * 
     * @return the key
     */
    short key();
 
    /**
     * Create a copy
     * @return return a clone of this pointer
     */
    MappeableContainerPointer clone();
    
}
