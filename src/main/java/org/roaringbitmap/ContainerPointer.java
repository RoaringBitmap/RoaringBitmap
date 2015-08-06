/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * 
 * This interface allows you to 
 * iterate over the containers in a roaring bitmap.
 *
 */
interface ContainerPointer extends Comparable<ContainerPointer>{
	/**
	 * This method can be used to check whether there is current a valid
	 * container as it returns null when there is not.
	 * @return null or the current container
	 */
	Container getContainer();
	
	/**
	 * Move to the next container
	 */
	void advance();
	
	/**
	 * The key is a 16-bit integer that indicates the position of
	 * the container in the roaring bitmap. To be interpreted as 
	 * an unsigned integer.
	 * @return the key
	 */
	short key();

    /**
     * Check whether the current container is a bitmap container.
     * 
     * @return whether it is a bitmap container
     */
    boolean isBitmapContainer();
    /**
     * Check whether the current container is a run container.
     * 
     * @return whether it is a run container
     */
    boolean isRunContainer();
    
    /**
     * Return the cardinality of the current container
     * @return the cardinality
     */
    int getCardinality();
}
