/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

/**
 * 
 * This interface allows you to iterate over the containers in a roaring bitmap.
 * 
 */
public interface MappeableContainerPointer
    extends Comparable<MappeableContainerPointer>, Cloneable {
  /**
   * Move to the next container
   */
  void advance();

  /**
   * Create a copy
   * 
   * @return return a clone of this pointer
   */
  MappeableContainerPointer clone();

  /**
   * Returns the cardinality of the current container. Can be faster than loading the container
   * first.
   * 
   * @return cardinality of the current container
   */
  int getCardinality();

  /**
   * This method can be used to check whether there is current a valid container as it returns null
   * when there is not.
   * 
   * @return null or the current container
   */
  MappeableContainer getContainer();

  /**
   * Get the size in bytes of the container. Used for sorting.
   * 
   * @return the size in bytes
   */
  int getSizeInBytes();

  /**
   * 
   * @return whether there is a container at the current position
   */
  boolean hasContainer();

  /**
   * Returns true if it is a bitmap container (MappeableBitmapContainer).
   * 
   * @return boolean indicated if it is a bitmap container
   */
  boolean isBitmapContainer();

  /**
   * Returns true if it is a run container (MappeableRunContainer).
   * 
   * @return boolean indicated if it is a run container
   */
  boolean isRunContainer();

  /**
   * The key is a 16-bit integer that indicates the position of the container in the roaring bitmap.
   * To be interpreted as an unsigned integer.
   * 
   * @return the key
   */
  char key();

  /**
   * Move to the previous container
   */
  void previous();


}
