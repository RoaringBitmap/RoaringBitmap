/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.io.DataOutput;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Interface representing an immutable bitmap.
 *
 */
public interface ImmutableBitmapDataProvider {
  /**
   * Checks whether the value in included, which is equivalent to checking if the corresponding bit
   * is set (get in BitSet class).
   *
   * @param x integer value
   * @return whether the integer value is included.
   */
  boolean contains(int x);

  /**
   * Returns the number of distinct integers added to the bitmap (e.g., number of bits set).
   * Internally, this is computed as a 64-bit number.
   *
   * @return the cardinality
   */
  int getCardinality();
  
  /**
   * Returns the number of distinct integers added to the bitmap (e.g., number of bits set).
   * This returns a full 64-bit result.
   *
   * @return the cardinality
   */
  long getLongCardinality();

  /**
   * Visit all values in the bitmap and pass them to the consumer.
   * 
   * * Usage: 
   * <pre>
   * {@code
   *  bitmap.forEach(new IntConsumer() {
   *
   *    {@literal @}Override
   *    public void accept(int value) {
   *      // do something here
   *      
   *    }});
   *   }
   * }
   * </pre>
   * @param ic the consumer
   */
  void forEach(IntConsumer ic);

  /**
   * For better performance, consider the Use the {@link #forEach forEach} method.
   * @return a custom iterator over set bits, the bits are traversed in ascending sorted order
   */
  PeekableIntIterator getIntIterator();

  /**
   * @return a custom iterator over set bits, the bits are traversed in descending sorted order
   */
  IntIterator getReverseIntIterator();

  /**
   * This iterator may be faster than others
   * @return iterator which works on batches of data.
   */
  BatchIterator getBatchIterator();

  /**
   * Estimate of the memory usage of this data structure.
   * 
   * Internally, this is computed as a 64-bit counter.
   *
   * @return estimated memory usage.
   */
  int getSizeInBytes();
  
  /**
   * Estimate of the memory usage of this data structure. Provides
   * full 64-bit number.
   *
   * @return estimated memory usage.
   */
  long getLongSizeInBytes();

  /**
   * Checks whether the bitmap is empty.
   *
   * @return true if this bitmap contains no set bit
   */
  boolean isEmpty();

  /**
   * Create a new bitmap of the same class, containing at most maxcardinality integers.
   *
   * @param x maximal cardinality
   * @return a new bitmap with cardinality no more than maxcardinality
   */
  ImmutableBitmapDataProvider limit(int x);

  /**
   * Rank returns the number of integers that are smaller or equal to x (rank(infinity) would be
   * getCardinality()).
   * 
   * The value is internally computed as a 64-bit number.
   * 
   * @param x upper limit
   *
   * @return the rank
   * @see <a href="https://en.wikipedia.org/wiki/Ranking#Ranking_in_statistics">Ranking in statistics</a> 
   */
  int rank(int x);
  
  /**
   * Rank returns the number of integers that are smaller or equal to x (rankLong(infinity) would be
   * getLongCardinality()).
   * Same as "rank" but produces a full 64-bit value.
   * 
   * @param x upper limit
   *
   * @return the rank
   * @see <a href="https://en.wikipedia.org/wiki/Ranking#Ranking_in_statistics">Ranking in statistics</a> 
   */
  long rankLong(int x);

  /**
   * Return the jth value stored in this bitmap. The provided value 
   * needs to be smaller than the cardinality otherwise an 
   * IllegalArgumentException
   * exception is thrown.
   *
   * @param j index of the value
   *
   * @return the value
   * @see <a href="https://en.wikipedia.org/wiki/Selection_algorithm">Selection algorithm</a> 
   */
  int select(int j);

  /**
   * Get the first (smallest) integer in this RoaringBitmap,
   * that is, returns the minimum of the set.
   * @return the first (smallest) integer
   * @throws NoSuchElementException if empty
   */
  int first();

  /**
   * Get the last (largest) integer in this RoaringBitmap,
   * that is, returns the maximum of the set.
   * @return the last (largest) integer
   * @throws NoSuchElementException if empty
   */
  int last();

  /**
   * Serialize this bitmap.
   *
   * The current bitmap is not modified.
   *
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void serialize(DataOutput out) throws IOException;

  /**
   * Report the number of bytes required to serialize this bitmap. This is the number of bytes
   * written out when using the serialize method. When using the writeExternal method, the count
   * will be higher due to the overhead of Java serialization.
   *
   * @return the size in bytes
   */
  int serializedSizeInBytes();

  /**
   * Return the set values as an array. The integer values are in sorted order.
   *
   * @return array representing the set values.
   */
  int[] toArray();

}
