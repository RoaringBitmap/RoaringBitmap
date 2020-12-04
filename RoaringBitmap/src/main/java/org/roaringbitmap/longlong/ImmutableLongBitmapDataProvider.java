/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.longlong;

import java.io.DataOutput;
import java.io.IOException;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

/**
 * Interface representing an immutable bitmap.
 *
 */
public interface ImmutableLongBitmapDataProvider {
  /**
   * Checks whether the value in included, which is equivalent to checking if the corresponding bit
   * is set (get in BitSet class).
   *
   * @param x long value
   * @return whether the long value is included.
   */
  public boolean contains(long x);

  /**
   * Returns the number of distinct integers added to the bitmap (e.g., number of bits set). This
   * returns a full 64-bit result.
   *
   * @return the cardinality
   */
  public long getLongCardinality();

  /**
   * Visit all values in the bitmap and pass them to the consumer.
   * 
   * * Usage:
   * 
   * <pre>
   * {@code
   *  bitmap.forEach(new LongConsumer() {
   *
   *    {@literal @}Override
   *    public void accept(long value) {
   *      // do something here
   *      
   *    }});
   *   }
   * }
   * </pre>
   * 
   * @param lc the consumer
   */
  public void forEach(LongConsumer lc);

  /**
   * For better performance, consider the Use the {@link #forEach forEach} method.
   * 
   * @return a custom iterator over set bits, the bits are traversed in ascending sorted order
   */
  // RoaringBitmap proposes a PeekableLongIterator
  public LongIterator getLongIterator();

  /**
   * @return a custom iterator over set bits, the bits are traversed in descending sorted order
   */
  // RoaringBitmap proposes a PeekableLongIterator
  public LongIterator getReverseLongIterator();

  /**
   * @return an Ordered, Distinct, Sorted and Sized IntStream in ascending order
   */
  public default LongStream stream() {
    int characteristics = Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SORTED 
        | Spliterator.SIZED;
    Spliterator.OfLong x = Spliterators.spliterator(new RoaringOfLong(getLongIterator()), 
        getLongCardinality(), characteristics);
    return StreamSupport.longStream(x, false);
  }

  /**
   * @return an Ordered, Distinct and Sized IntStream providing bits in descending sorted order
   */
  public default LongStream reverseStream() {
    int characteristics = Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SIZED;
    Spliterator.OfLong x = Spliterators.spliterator(new RoaringOfLong(getLongIterator()), 
        getLongCardinality(), characteristics);
    return StreamSupport.longStream(x, false);
  }
  /**
   * Estimate of the memory usage of this data structure.
   * 
   * Internally, this is computed as a 64-bit counter.
   *
   * @return estimated memory usage.
   */
  public int getSizeInBytes();

  /**
   * Estimate of the memory usage of this data structure. Provides full 64-bit number.
   *
   * @return estimated memory usage.
   */
  public long getLongSizeInBytes();

  /**
   * Checks whether the bitmap is empty.
   *
   * @return true if this bitmap contains no set bit
   */
  public boolean isEmpty();

  /**
   * Create a new bitmap of the same class, containing at most maxcardinality integers.
   *
   * @param x maximal cardinality
   * @return a new bitmap with cardinality no more than maxcardinality
   */
  public ImmutableLongBitmapDataProvider limit(long x);

  /**
   * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be
   * GetCardinality()).
   * 
   * The value is a full 64-bit value.
   * 
   * @param x upper limit
   *
   * @return the rank
   */
  public long rankLong(long x);

  /**
   * Return the jth value stored in this bitmap.
   *
   * @param j index of the value
   *
   * @return the value
   */
  public long select(long j);

  /**
   * Serialize this bitmap.
   *
   * The current bitmap is not modified.
   *
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void serialize(DataOutput out) throws IOException;

  /**
   * Report the number of bytes required to serialize this bitmap. This is the number of bytes
   * written out when using the serialize method. When using the writeExternal method, the count
   * will be higher due to the overhead of Java serialization.
   *
   * @return the size in bytes
   */
  public long serializedSizeInBytes();

  /**
   * Return the set values as an array. The integer values are in sorted order.
   *
   * @return array representing the set values.
   */
  public long[] toArray();

    /**
   * An internal class to help provide streams.
   * Sad but true the interface of IntIterator and PrimitiveIterator.OfInt
   * Does not match. Otherwise it would be easier to just make IntIterator 
   * implement PrimitiveIterator.OfInt. 
   */
  static final class RoaringOfLong implements PrimitiveIterator.OfLong {
    private final LongIterator iterator;

    public RoaringOfLong(LongIterator iterator) {
      this.iterator = iterator;
    }

    @Override
    public long nextLong() {
      return iterator.next();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }
  }
}
