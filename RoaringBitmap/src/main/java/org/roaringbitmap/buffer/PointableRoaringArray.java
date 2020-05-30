/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Generic interface for the array underlying roaring bitmap classes.
 * 
 */
public interface PointableRoaringArray extends Cloneable {
  /**
   * Find the smallest integer index larger than pos such that getKeyAtIndex(index)&gt;=x. If none
   * can be found, return size.
   *
   * @param x minimal value
   * @param pos index to exceed
   * @return the smallest index greater than pos such that getKeyAtIndex(index) is at least as large
   *         as min, or size if it is not possible.
   */
  int advanceUntil(char x, int pos);

  /**
   * Create an independent copy of the underlying array
   * 
   * @return a copy
   */
  PointableRoaringArray clone();

  /**
   * This checks whether the container at index i has the value x.
   * This can be faster than calling "getContainerAtIndex" and then calling
   * contains.
   * 
   * @param i container index (assumed to be non-negative)
   * @param x 16-bit value to check
   * @return whether the container contains at index i contains x
   */
  boolean containsForContainerAtIndex(int i, char x);

  
  /**
   * Returns the cardinality of the container at the given index. This method is expected to be
   * fast.
   * 
   * @param i index
   * @return the cardinality
   */
  int getCardinality(int i);

  /**
   * Obsolete method (retired because it forces us to create a new container).
   * 
   * @param x 16-bit key
   * @return matching container
   */
  //MappeableContainer getContainer(short x);

  
   /**
   * Returns either the index of the container corresponding to key x, or a negative value.
   * @param x 16-bit key
   * @return index of container (negative value if no container found)
   */
  int getContainerIndex(char x);

  /**
   * @param i index
   * @return matching container
   */
  MappeableContainer getContainerAtIndex(int i);

  /**
   * @return a ContainerPointer to iterator over the array
   */
  MappeableContainerPointer getContainerPointer();

  /**
   * @param startIndex starting index
   * @return a ContainerPointer to iterator over the array initially positioned at startIndex
   */
  MappeableContainerPointer getContainerPointer(int startIndex);

  /**
   * @param x 16-bit key
   * @return corresponding index
   */
  int getIndex(char x);

  /**
   * @param i the index
   * @return 16-bit key at the index
   */
  char getKeyAtIndex(int i);

  /**
   * Check whether this bitmap has had its runs compressed.
   * 
   * @return whether this bitmap has run compression
   */
  boolean hasRunCompression();

  /**
   * Serialize.
   * 
   * The current bitmap is not modified.
   * 
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void serialize(DataOutput out) throws IOException;

  /**
   * Serialize.
   *
   * The current bitmap is not modified.
   *
   * @param buffer the buffer to serialize to
   */
  void serialize(ByteBuffer buffer);

  /**
   * @return the size that the data structure occupies on disk
   */
  int serializedSizeInBytes();


  /**
   * @return number of keys
   */
  int size();

  /**
   * Gets the first value in the array
   * @return te first value in the array
   */
  int first();

  /**
   * Gets the last value in the array
   * @return te last value in the array
   */
  int last();
}
