/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

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
   * @return an Ordered, Distinct, Sorted and Sized IntStream in ascending order
   */
  public default IntStream stream() {
    int characteristics = Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SORTED 
        | Spliterator.SIZED;
    Spliterator.OfInt x = Spliterators.spliterator(new RoaringOfInt(getIntIterator()), 
        getCardinality(), characteristics);
    return StreamSupport.intStream(x, false);
  }

  /**
   * @return an Ordered, Distinct and Sized IntStream providing bits in descending sorted order
   */
  public default IntStream reverseStream() {
    int characteristics = Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SIZED;
    Spliterator.OfInt x = Spliterators.spliterator(new RoaringOfInt(getReverseIntIterator()), 
        getCardinality(), characteristics);
    return StreamSupport.intStream(x, false);
  }
  
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
   * getCardinality()).  If you provide the smallest value as a parameter, this function will
   * return 1. If provide a value smaller than the smallest value, it will return 0.
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
   * getLongCardinality()).  If you provide the smallest value as a parameter, this function will
   * return 1. If provide a value smaller than the smallest value, it will return 0.
   * Same as "rank" but produces a full 64-bit value.
   * 
   * @param x upper limit
   *
   * @return the rank
   * @see <a href="https://en.wikipedia.org/wiki/Ranking#Ranking_in_statistics">Ranking in statistics</a> 
   */
  long rankLong(int x);

  /**
  * Computes the number of values in the interval [start,end) where
  * start is included and end excluded.
  * rangeCardinality(0,0x100000000) provides the total cardinality (getLongCardinality).
  * The answer is a 64-bit value between 1 and 0x100000000. 
  * 
  * @param start lower limit (included)
  * @param end upper limit (excluded)
  * @return the number of elements in [start,end), between 0 and 0x100000000.
  */
  long rangeCardinality(long start, long end);

  /**
   * Return the jth value stored in this bitmap. The provided value 
   * needs to be smaller than the cardinality otherwise an 
   * IllegalArgumentException
   * exception is thrown. The smallest value is at index 0.
   * Note that this function differs in convention from the rank function which
   * returns 1 when ranking the smallest value.
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
   * Returns the first value equal to or larger than the provided value
   * (interpreted as an unsigned integer). If no such
   * bit exists then {@code -1} is returned. It is not necessarily a
   * computationally effective way to iterate through the values.
   *
   * @param  fromValue the lower bound (inclusive)
   * @return the smallest value larger than or equal to the specified value,
   *       or {@code -1} if there is no such value
   */
  long nextValue(int fromValue);
  
  /**
   * Returns the first value less than or equal to the provided value
   * (interpreted as an unsigned integer). If no such
   * bit exists then {@code -1} is returned. It is not an efficient
   * way to iterate through the values backwards.
   *
   * @param  fromValue the upper bound (inclusive)
   * @return the largest value less than or equal to the specified value,
   *       or {@code -1} if there is no such value
   */
  long previousValue(int fromValue);

  /**
   * Returns the first absent value equal to or larger than the provided
   * value (interpreted as an unsigned integer). It is not necessarily a
   * computationally effective way to iterate through the values.
   *
   * @param  fromValue the lower bound (inclusive)
   * @return the smallest absent value larger than or equal to the specified
   *       value.
   */
  long nextAbsentValue(int fromValue);

  /**
   * Returns the first absent value less than or equal to the provided
   * value (interpreted as an unsigned integer). It is not necessarily a
   * computationally effective way to iterate through the values.
   *
   * @param  fromValue the lower bound (inclusive)
   * @return the smallest absent value larger than or equal to the specified
   *       value.
   */
  long previousAbsentValue(int fromValue);

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
   * Serialize this bitmap to a ByteBuffer.
   * This is the preferred method
   * to serialize to a byte array (byte[]) or to a String 
   * (via Base64.getEncoder().encodeToString)..
   *
   *  
   * Irrespective of the endianess of the provided buffer, data is 
   * written using LITTlE_ENDIAN as per the RoaringBitmap specification.
   *
   * The current bitmap is not modified.
   * <pre>
   * {@code
   *   byte[] array = new byte[mrb.serializedSizeInBytes()];
   *   mrb.serialize(ByteBuffer.wrap(array));
   * }
   * </pre>
   *
   * @param buffer the ByteBuffer
   */
  void serialize(ByteBuffer buffer);

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

  /**
   * An internal class to help provide streams.
   * Sad but true the interface of IntIterator and PrimitiveIterator.OfInt
   * Does not match. Otherwise it would be easier to just make IntIterator 
   * implement PrimitiveIterator.OfInt. 
   */
  static final class RoaringOfInt implements PrimitiveIterator.OfInt {
    private final IntIterator iterator;

    public RoaringOfInt(IntIterator iterator) {
      this.iterator = iterator;
    }

    @Override
    public int nextInt() {
      return iterator.next();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }
  }
}
