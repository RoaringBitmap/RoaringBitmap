/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import org.roaringbitmap.Util;

import static java.lang.Long.numberOfTrailingZeros;

/**
 * Various useful methods for roaring bitmaps.
 *
 * This class is similar to org.roaringbitmap.Util but meant to be used with memory mapping.
 */
public final class BufferUtil {

  /**
   * Add value "offset" to all values in the container, producing
   * two new containers. The existing container remains unchanged.
   * The new container are not converted, so they need to be checked:
   * e.g., we could produce two bitmap containers having low cardinality.
   * @param source source container
   * @param offsets value to add to each value in the container
   * @return return an array made of two containers
   */
  public static  MappeableContainer[] addOffset(MappeableContainer source, short offsets) {
    final int offset = BufferUtil.toIntUnsigned(offsets);
    // could be a whole lot faster, this is a simple implementation
    if(source instanceof MappeableArrayContainer) {
      MappeableArrayContainer c = (MappeableArrayContainer) source;
      MappeableArrayContainer low = new MappeableArrayContainer(c.cardinality);
      MappeableArrayContainer high = new MappeableArrayContainer(c.cardinality);
      for(int k = 0; k < c.cardinality; k++) {
        int val = BufferUtil.toIntUnsigned(c.content.get(k));
        val += offset;
        if(val <= 0xFFFF) {
          low.content.put(low.cardinality++, (short) val);
        } else {
          high.content.put(high.cardinality++, (short) (val & 0xFFFF));
        }
      }
      return new MappeableContainer[] {low, high};
    } else if (source instanceof MappeableBitmapContainer) {
      MappeableBitmapContainer c = (MappeableBitmapContainer) source;
      MappeableBitmapContainer low = new MappeableBitmapContainer();
      MappeableBitmapContainer high = new MappeableBitmapContainer();
      low.cardinality = -1;
      high.cardinality = -1;
      final int b = offset >>> 6;
      final int i = offset % 64;
      if(i == 0) {
        for(int k = 0; k < 1024 - b; k++) {
          low.bitmap.put(b + k, c.bitmap.get(k));
        }
        for(int k = 1024 - b; k < 1024 ; k++) {
          high.bitmap.put(k - (1024 - b),c.bitmap.get(k));
        }
      } else {
        low.bitmap.put(b + 0, c.bitmap.get(0) << i);
        for(int k = 1; k < 1024 - b; k++) {
          low.bitmap.put(b + k, (c.bitmap.get(k) << i) 
              | (c.bitmap.get(k - 1) >>> (64-i)));
        }
        for(int k = 1024 - b; k < 1024 ; k++) {
          high.bitmap.put(k - (1024 - b),
               (c.bitmap.get(k) << i) 
               | (c.bitmap.get(k - 1) >>> (64-i)));
        }
        high.bitmap.put(b,  (c.bitmap.get(1024 - 1) >>> (64-i)));
      }
      return new MappeableContainer[] {low.repairAfterLazy(), high.repairAfterLazy()};
    } else if (source instanceof MappeableRunContainer) {
      MappeableRunContainer c = (MappeableRunContainer) source;
      MappeableRunContainer low = new MappeableRunContainer();
      MappeableRunContainer high = new MappeableRunContainer();
      for(int k = 0 ; k < c.nbrruns; k++) {
        int val =  BufferUtil.toIntUnsigned(c.getValue(k));
        val += offset;
        int finalval =  val + BufferUtil.toIntUnsigned(c.getLength(k));
        if(val <= 0xFFFF) {
          if(finalval <= 0xFFFF) {
            low.smartAppend((short)val,c.getLength(k));
          } else {
            low.smartAppend((short)val,(short)(0xFFFF-val));
            high.smartAppend((short) 0,(short)(finalval & 0xFFFF));
          }
        } else {
          high.smartAppend((short)(val & 0xFFFF),c.getLength(k));
        }
      }
      return new MappeableContainer[] {low, high};
    }
    throw new RuntimeException("unknown container type"); // never happens
  }

  /**
   * Find the smallest integer larger than pos such that array[pos]&gt;= min. If none can be found,
   * return length. Based on code by O. Kaser.
   *
   * @param array container where we search
   * @param pos initial position
   * @param min minimal threshold
   * @param length how big should the array consider to be
   * @return x greater than pos such that array[pos] is at least as large as min, pos is is equal to
   *         length if it is not possible.
   */
  protected static int advanceUntil(ShortBuffer array, int pos, int length, short min) {
    int lower = pos + 1;

    // special handling for a possibly common sequential case
    if (lower >= length || toIntUnsigned(array.get(lower)) >= toIntUnsigned(min)) {
      return lower;
    }

    int spansize = 1; // could set larger
    // bootstrap an upper limit

    while (lower + spansize < length
        && toIntUnsigned(array.get(lower + spansize)) < toIntUnsigned(min)) {
      spansize *= 2; // hoping for compiler will reduce to
    }
    // shift
    int upper = (lower + spansize < length) ? lower + spansize : length - 1;

    // maybe we are lucky (could be common case when the seek ahead
    // expected
    // to be small and sequential will otherwise make us look bad)
    if (array.get(upper) == min) {
      return upper;
    }

    if (toIntUnsigned(array.get(upper)) < toIntUnsigned(min)) {// means
      // array
      // has no
      // item
      // >= min
      // pos = array.length;
      return length;
    }

    // we know that the next-smallest span was too small
    lower += (spansize >>> 1);

    // else begin binary search
    // invariant: array[lower]<min && array[upper]>min
    while (lower + 1 != upper) {
      int mid = (lower + upper) >>> 1;
      short arraymid = array.get(mid);
      if (arraymid == min) {
        return mid;
      } else if (toIntUnsigned(arraymid) < toIntUnsigned(min)) {
        lower = mid;
      } else {
        upper = mid;
      }
    }
    return upper;

  }

  /**
   * Find the smallest integer larger than pos such that array[pos]&gt;= min. If none can be found,
   * return length.
   *
   * @param array array to search within
   * @param pos starting position of the search
   * @param length length of the array to search
   * @param min minimum value
   * @return x greater than pos such that array[pos] is at least as large as min, pos is is equal to
   *         length if it is not possible.
   */
  public static int iterateUntil(ShortBuffer array, int pos, int length, int min) {
    while (pos < length && toIntUnsigned(array.get(pos)) < min) {
      pos++;
    }
    return pos;
  }


  protected static void arraycopy(ShortBuffer src, int srcPos, ShortBuffer dest, int destPos,
      int length) {
    if (BufferUtil.isBackedBySimpleArray(src) && BufferUtil.isBackedBySimpleArray(dest)) {
      System.arraycopy(src.array(), srcPos, dest.array(), destPos, length);
    } else {
      if (srcPos < destPos) {
        for (int k = length - 1; k >= 0; --k) {
          dest.put(destPos + k, src.get(k + srcPos));
        }
      } else {
        for (int k = 0; k < length; ++k) {
          dest.put(destPos + k, src.get(k + srcPos));
        }
      }
    }
  }

  protected static int branchyUnsignedBinarySearch(final ShortBuffer array, final int begin,
      final int end, final short k) {
    final int ikey = toIntUnsigned(k);
    // next line accelerates the possibly common case where the value would be inserted at the end
    if ((end > 0) && (toIntUnsigned(array.get(end - 1)) < ikey)) {
      return -end - 1;
    }
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = toIntUnsigned(array.get(middleIndex));

      if (middleValue < ikey) {
        low = middleIndex + 1;
      } else if (middleValue > ikey) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    return -(low + 1);
  }


  protected static int branchyUnsignedBinarySearch(final ByteBuffer array, int position,
        final int begin, final int end, final short k) {
    final int ikey = toIntUnsigned(k);
    // next line accelerates the possibly common case where the value would be inserted at the end
    if ((end > 0) && (toIntUnsigned(array.getShort(position + (end - 1)*2)) < ikey)) {
      return -end - 1;
    }
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = toIntUnsigned(array.getShort(position + 2* middleIndex));

      if (middleValue < ikey) {
        low = middleIndex + 1;
      } else if (middleValue > ikey) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    return -(low + 1);
  }

  /**
   * Compares the two specified {@code short} values, treating them as unsigned values between
   * {@code 0} and {@code 2^16 - 1} inclusive.
   *
   * @param a the first unsigned {@code short} to compare
   * @param b the second unsigned {@code short} to compare
   * @return a negative value if {@code a} is less than {@code b}; a positive value if {@code a} is
   *         greater than {@code b}; or zero if they are equal
   */
  public static int compareUnsigned(short a, short b) {
    return toIntUnsigned(a) - toIntUnsigned(b);
  }

  protected static void fillArrayAND(short[] container, LongBuffer bitmap1, LongBuffer bitmap2) {
    int pos = 0;
    if (bitmap1.limit() != bitmap2.limit()) {
      throw new IllegalArgumentException("not supported");
    }
    if (BufferUtil.isBackedBySimpleArray(bitmap1) && BufferUtil.isBackedBySimpleArray(bitmap2)) {
      int len = bitmap1.limit();
      long[] b1 = bitmap1.array();
      long[] b2 = bitmap2.array();
      for (int k = 0; k < len; ++k) {
        long bitset = b1[k] & b2[k];
        while (bitset != 0) {
          container[pos++] = (short) (k * 64 + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
      }
    } else {
      int len = bitmap1.limit();
      for (int k = 0; k < len; ++k) {
        long bitset = bitmap1.get(k) & bitmap2.get(k);
        while (bitset != 0) {
          container[pos++] = (short) (k * 64 + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
      }
    }
  }

  protected static void fillArrayANDNOT(short[] container, LongBuffer bitmap1, LongBuffer bitmap2) {
    int pos = 0;
    if (bitmap1.limit() != bitmap2.limit()) {
      throw new IllegalArgumentException("not supported");
    }
    if (BufferUtil.isBackedBySimpleArray(bitmap1) && BufferUtil.isBackedBySimpleArray(bitmap2)) {
      int len = bitmap1.limit();
      long[] b1 = bitmap1.array();
      long[] b2 = bitmap2.array();
      for (int k = 0; k < len; ++k) {
        long bitset = b1[k] & (~b2[k]);
        while (bitset != 0) {
          container[pos++] = (short) (k * 64 + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
      }
    } else {
      int len = bitmap1.limit();
      for (int k = 0; k < len; ++k) {
        long bitset = bitmap1.get(k) & (~bitmap2.get(k));
        while (bitset != 0) {
          container[pos++] = (short) (k * 64 + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
      }
    }
  }

  protected static void fillArrayXOR(short[] container, LongBuffer bitmap1, LongBuffer bitmap2) {
    int pos = 0;
    if (bitmap1.limit() != bitmap2.limit()) {
      throw new IllegalArgumentException("not supported");
    }
    if (BufferUtil.isBackedBySimpleArray(bitmap1) && BufferUtil.isBackedBySimpleArray(bitmap2)) {
      org.roaringbitmap.Util.fillArrayXOR(container, bitmap1.array(),  bitmap2.array());
    } else {
      int len = bitmap1.limit();
      for (int k = 0; k < len; ++k) {
        long bitset = bitmap1.get(k) ^ bitmap2.get(k);
        while (bitset != 0) {
          container[pos++] = (short) (k * 64 + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
      }
    }
  }


  /**
   * flip bits at start, start+1,..., end-1
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   */
  public static void flipBitmapRange(LongBuffer bitmap, int start, int end) {
    if (isBackedBySimpleArray(bitmap)) {
      Util.flipBitmapRange(bitmap.array(), start, end);
      return;
    }
    if (start == end) {
      return;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;
    bitmap.put(firstword, bitmap.get(firstword) ^ ~(~0L << start));
    for (int i = firstword; i < endword; i++) {
      bitmap.put(i, ~bitmap.get(i));
    }
    bitmap.put(endword, bitmap.get(endword) ^ (~0L >>> -end));
  }


  /**
   * Hamming weight of the 64-bit words involved in the range start, start+1,..., end-1
   * that is, it will compute the cardinality of the bitset from index
   * (floor(start/64) to floor((end-1)/64)) inclusively.
   *
   * @param bitmap array of words representing a bitset
   * @param start first index (inclusive)
   * @param end last index (exclusive)
   * @return the hamming weight of the corresponding words
   */
  @Deprecated
  private static int cardinalityInBitmapWordRange(LongBuffer bitmap, int start, int end) {
    if (isBackedBySimpleArray(bitmap)) {
      return Util.cardinalityInBitmapWordRange(bitmap.array(), start, end);
    }
    if (start >= end) {
      return 0;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;
    int answer = 0;
    for (int i = firstword; i <= endword; i++) {
      answer += Long.bitCount(bitmap.get(i));
    }
    return answer;
  }

  /**
   * Hamming weight of the bitset in the range
   *  start, start+1,..., end-1
   *
   * @param bitmap array of words representing a bitset
   * @param start first index  (inclusive)
   * @param end last index  (exclusive)
   * @return the hamming weight of the corresponding range
   */
  public static int cardinalityInBitmapRange(LongBuffer bitmap, int start, int end) {
    if (isBackedBySimpleArray(bitmap)) {
      return Util.cardinalityInBitmapRange(bitmap.array(), start, end);
    }
    if (start >= end) {
      return 0;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;
    if (firstword == endword) {
      return Long.bitCount(bitmap.get(firstword) & ((~0L << start) & (~0L >>> -end)));
    }
    int answer = Long.bitCount(bitmap.get(firstword) & (~0L << start));
    for (int i = firstword + 1; i < endword; i++) {
      answer += Long.bitCount(bitmap.get(i));
    }
    answer += Long.bitCount(bitmap.get(endword) & (~0L >>> -end));
    return answer;
  }


  /**
   * set bits at start, start+1,..., end-1 and report the cardinality change
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   * @return cardinality change
   */
  @Deprecated
  public static int setBitmapRangeAndCardinalityChange(LongBuffer bitmap, int start, int end) {
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      return Util.setBitmapRangeAndCardinalityChange(bitmap.array(), start, end);
    }
    int cardbefore = cardinalityInBitmapWordRange(bitmap, start, end);
    setBitmapRange(bitmap, start, end);
    int cardafter = cardinalityInBitmapWordRange(bitmap, start, end);
    return cardafter - cardbefore;
  }


  /**
   * flip bits at start, start+1,..., end-1 and report the cardinality change
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   * @return cardinality change
   */
  @Deprecated
  public static int flipBitmapRangeAndCardinalityChange(LongBuffer bitmap, int start, int end) {
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      return Util.flipBitmapRangeAndCardinalityChange(bitmap.array(), start, end);
    }
    int cardbefore = cardinalityInBitmapWordRange(bitmap, start, end);
    flipBitmapRange(bitmap, start, end);
    int cardafter = cardinalityInBitmapWordRange(bitmap, start, end);
    return cardafter - cardbefore;
  }


  /**
   * reset bits at start, start+1,..., end-1 and report the cardinality change
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   * @return cardinality change
   */
  @Deprecated
  public static int resetBitmapRangeAndCardinalityChange(LongBuffer bitmap, int start, int end) {
    if (BufferUtil.isBackedBySimpleArray(bitmap)) {
      return Util.resetBitmapRangeAndCardinalityChange(bitmap.array(), start, end);
    }
    int cardbefore = cardinalityInBitmapWordRange(bitmap, start, end);
    resetBitmapRange(bitmap, start, end);
    int cardafter = cardinalityInBitmapWordRange(bitmap, start, end);
    return cardafter - cardbefore;
  }

  /**
   * From the cardinality of a container, compute the corresponding size in bytes of the container.
   * Additional information is required if the container is run encoded.
   *
   * @param card the cardinality if this is not run encoded, otherwise ignored
   * @param numRuns number of runs if run encoded, othewise ignored
   * @param isRunEncoded boolean
   *
   * @return the size in bytes
   */
  protected static int getSizeInBytesFromCardinalityEtc(int card, int numRuns,
      boolean isRunEncoded) {
    if (isRunEncoded) {
      return 2 + numRuns * 2 * 2; // each run uses 2 shorts, plus the initial short giving num runs
    }
    boolean isBitmap = card > MappeableArrayContainer.DEFAULT_MAX_SIZE;
    if (isBitmap) {
      return MappeableBitmapContainer.MAX_CAPACITY / 8;
    } else {
      return card * 2;
    }

  }

  protected static short highbits(int x) {
    return (short) (x >>> 16);
  }


  protected static short highbits(long x) {
    return (short) (x >>> 16);
  }

  /**
   * Checks whether the Buffer is backed by a simple array. In java, a Buffer is an abstraction that
   * can represent various data, from data on disk all the way to native Java arrays. Like all
   * abstractions, a Buffer might carry a performance penalty. Thus, we sometimes check whether the
   * Buffer is simply a wrapper around a Java array. In these instances, it might be best, from a
   * performance point of view, to access the underlying array (using the array()) method.
   * @param b the provided Buffer
   * @return whether the Buffer is backed by a simple array
   */
  protected static boolean isBackedBySimpleArray(Buffer b) {
    return b.hasArray() && (b.arrayOffset() == 0);
  }

  protected static short lowbits(int x) {
    return (short) (x & 0xFFFF);
  }

  protected static short lowbits(long x) {
    return (short) (x & 0xFFFF);
  }

  protected static int lowbitsAsInteger(int x) {
    return x & 0xFFFF;
  }

  protected static int lowbitsAsInteger(long x) {
    return (int)(x & 0xFFFF);
  }

  protected static long lowbitsAsLong(long x) {
    return x & 0xFFFF;
  }

  protected static short maxLowBit() {
    return (short) 0xFFFF;
  }

  protected static int maxLowBitAsInteger() {
    return 0xFFFF;
  }

  /**
   * clear bits at start, start+1,..., end-1
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   */
  public static void resetBitmapRange(LongBuffer bitmap, int start, int end) {
    if (isBackedBySimpleArray(bitmap)) {
      Util.resetBitmapRange(bitmap.array(), start, end);
      return;
    }
    if (start == end) {
      return;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;
    if (firstword == endword) {
      bitmap.put(firstword, bitmap.get(firstword) & ~((~0L << start) & (~0L >>> -end)));
      return;
    }
    bitmap.put(firstword, bitmap.get(firstword) & (~(~0L << start)));
    for (int i = firstword + 1; i < endword; i++) {
      bitmap.put(i, 0L);
    }
    bitmap.put(endword, bitmap.get(endword) & (~(~0L >>> -end)));
  }


  /**
   * set bits at start, start+1,..., end-1
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   */
  public static void setBitmapRange(LongBuffer bitmap, int start, int end) {
    if (isBackedBySimpleArray(bitmap)) {
      Util.setBitmapRange(bitmap.array(), start, end);
      return;
    }
    if (start == end) {
      return;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;
    if (firstword == endword) {
      bitmap.put(firstword, bitmap.get(firstword) | ((~0L << start) & (~0L >>> -end)));

      return;
    }
    bitmap.put(firstword, bitmap.get(firstword) | (~0L << start));
    for (int i = firstword + 1; i < endword; i++) {
      bitmap.put(i, ~0L);
    }
    bitmap.put(endword, bitmap.get(endword) | (~0L >>> -end));
  }


  protected static int toIntUnsigned(short x) {
    return x & 0xFFFF;
  }

  /**
   * Look for value k in buffer in the range [begin,end). If the value is found, return its index.
   * If not, return -(i+1) where i is the index where the value would be inserted. The buffer is
   * assumed to contain sorted values where shorts are interpreted as unsigned integers.
   *
   * @param array buffer where we search
   * @param begin first index (inclusive)
   * @param end last index (exclusive)
   * @param k value we search for
   * @return count
   */
  public static int unsignedBinarySearch(final ShortBuffer array, final int begin, final int end,
      final short k) {
    return branchyUnsignedBinarySearch(array, begin, end, k);
  }
  /**
   * Look for value k in buffer in the range [begin,end). If the value is found, return its index.
   * If not, return -(i+1) where i is the index where the value would be inserted. The buffer is
   * assumed to contain sorted values where shorts are interpreted as unsigned integers.
   *
   * @param array buffer where we search
   * @param position starting position of the container in the ByteBuffer
   * @param begin first index (inclusive)
   * @param end last index (exclusive)
   * @param k value we search for
   * @return count
   */
  public static int unsignedBinarySearch(final ByteBuffer array, int position,
      final int begin, final int end, final short k) {
    return branchyUnsignedBinarySearch(array, position, begin, end, k);
  }

  protected static int unsignedDifference(final ShortBuffer set1, final int length1,
      final ShortBuffer set2, final int length2, final short[] buffer) {
    int pos = 0;
    int k1 = 0, k2 = 0;
    if (0 == length2) {
      set1.get(buffer, 0, length1);
      return length1;
    }
    if (0 == length1) {
      return 0;
    }
    short s1 = set1.get(k1);
    short s2 = set2.get(k2);
    while (true) {
      if (toIntUnsigned(s1) < toIntUnsigned(s2)) {
        buffer[pos++] = s1;
        ++k1;
        if (k1 >= length1) {
          break;
        }
        s1 = set1.get(k1);
      } else if (toIntUnsigned(s1) == toIntUnsigned(s2)) {
        ++k1;
        ++k2;
        if (k1 >= length1) {
          break;
        }
        if (k2 >= length2) {
          set1.position(k1);
          set1.get(buffer, pos, length1 - k1);
          return pos + length1 - k1;
        }
        s1 = set1.get(k1);
        s2 = set2.get(k2);
      } else {// if (val1>val2)
        ++k2;
        if (k2 >= length2) {
          set1.position(k1);
          set1.get(buffer, pos, length1 - k1);
          return pos + length1 - k1;
        }
        s2 = set2.get(k2);
      }
    }
    return pos;
  }

  protected static int unsignedExclusiveUnion2by2(final ShortBuffer set1, final int length1,
      final ShortBuffer set2, final int length2, final short[] buffer) {
    int pos = 0;
    int k1 = 0, k2 = 0;
    if (0 == length2) {
      set1.get(buffer, 0, length1);
      return length1;
    }
    if (0 == length1) {
      set2.get(buffer, 0, length2);
      return length2;
    }
    short s1 = set1.get(k1);
    short s2 = set2.get(k2);
    while (true) {
      if (toIntUnsigned(s1) < toIntUnsigned(s2)) {
        buffer[pos++] = s1;
        ++k1;
        if (k1 >= length1) {
          set2.position(k2);
          set2.get(buffer, pos, length2 - k2);
          return pos + length2 - k2;
        }
        s1 = set1.get(k1);
      } else if (toIntUnsigned(s1) == toIntUnsigned(s2)) {
        ++k1;
        ++k2;
        if (k1 >= length1) {
          set2.position(k2);
          set2.get(buffer, pos, length2 - k2);
          return pos + length2 - k2;
        }
        if (k2 >= length2) {
          set1.position(k1);
          set1.get(buffer, pos, length1 - k1);
          return pos + length1 - k1;
        }
        s1 = set1.get(k1);
        s2 = set2.get(k2);
      } else {// if (val1>val2)
        buffer[pos++] = s2;
        ++k2;
        if (k2 >= length2) {
          set1.position(k1);
          set1.get(buffer, pos, length1 - k1);
          return pos + length1 - k1;
        }
        s2 = set2.get(k2);
      }
    }
    // return pos;
  }


  protected static int unsignedIntersect2by2(final ShortBuffer set1, final int length1,
      final ShortBuffer set2, final int length2, final short[] buffer) {
    final int THRESHOLD = 34;
    if (length1 * THRESHOLD < length2) {
      return unsignedOneSidedGallopingIntersect2by2(set1, length1, set2, length2, buffer);
    } else if (length2 * THRESHOLD < length1) {
      return unsignedOneSidedGallopingIntersect2by2(set2, length2, set1, length1, buffer);
    } else {
      return unsignedLocalIntersect2by2(set1, length1, set2, length2, buffer);
    }
  }

  /**
   * Checks if two arrays intersect
   *
   * @param set1 first array
   * @param length1 length of first array
   * @param set2 second array
   * @param length2 length of second array
   * @return true if they intersect
   */
  public static boolean unsignedIntersects(ShortBuffer set1, int length1, ShortBuffer set2,
      int length2) {
    if ((0 == length1) || (0 == length2)) {
      return false;
    }
    int k1 = 0;
    int k2 = 0;

    // could be more efficient with galloping
    short s1 = set1.get(k1);
    short s2 = set2.get(k2);

    mainwhile: while (true) {
      if (toIntUnsigned(s2) < toIntUnsigned(s1)) {
        do {
          ++k2;
          if (k2 == length2) {
            break mainwhile;
          }
          s2 = set2.get(k2);
        } while (toIntUnsigned(s2) < toIntUnsigned(s1));
      }
      if (toIntUnsigned(s1) < toIntUnsigned(s2)) {
        do {
          ++k1;
          if (k1 == length1) {
            break mainwhile;
          }
          s1 = set1.get(k1);
        } while (toIntUnsigned(s1) < toIntUnsigned(s2));
      } else {
        return true;
      }
    }
    return false;
  }

  protected static int unsignedLocalIntersect2by2(final ShortBuffer set1, final int length1,
      final ShortBuffer set2, final int length2, final short[] buffer) {
    if ((0 == length1) || (0 == length2)) {
      return 0;
    }
    int k1 = 0;
    int k2 = 0;
    int pos = 0;
    short s1 = set1.get(k1);
    short s2 = set2.get(k2);

    mainwhile: while (true) {
      if (toIntUnsigned(s2) < toIntUnsigned(s1)) {
        do {
          ++k2;
          if (k2 == length2) {
            break mainwhile;
          }
          s2 = set2.get(k2);

        } while (toIntUnsigned(s2) < toIntUnsigned(s1));
      }
      if (toIntUnsigned(s1) < toIntUnsigned(s2)) {
        do {
          ++k1;
          if (k1 == length1) {
            break mainwhile;
          }
          s1 = set1.get(k1);

        } while (toIntUnsigned(s1) < toIntUnsigned(s2));
      } else {
        // (set2.get(k2) == set1.get(k1))
        buffer[pos++] = s1;
        ++k1;
        if (k1 == length1) {
          break;
        }
        s1 = set1.get(k1);
        ++k2;
        if (k2 == length2) {
          break;
        }
        s2 = set2.get(k2);

      }
    }
    return pos;
  }

  protected static int unsignedLocalIntersect2by2Cardinality(final ShortBuffer set1,
      final int length1, final ShortBuffer set2, final int length2) {
    if ((0 == length1) || (0 == length2)) {
      return 0;
    }
    int k1 = 0;
    int k2 = 0;
    int pos = 0;
    short s1 = set1.get(k1);
    short s2 = set2.get(k2);

    mainwhile: while (true) {
      if (toIntUnsigned(s2) < toIntUnsigned(s1)) {
        do {
          ++k2;
          if (k2 == length2) {
            break mainwhile;
          }
          s2 = set2.get(k2);

        } while (toIntUnsigned(s2) < toIntUnsigned(s1));
      }
      if (toIntUnsigned(s1) < toIntUnsigned(s2)) {
        do {
          ++k1;
          if (k1 == length1) {
            break mainwhile;
          }
          s1 = set1.get(k1);

        } while (toIntUnsigned(s1) < toIntUnsigned(s2));
      } else {
        ++pos;
        ++k1;
        if (k1 == length1) {
          break;
        }
        s1 = set1.get(k1);
        ++k2;
        if (k2 == length2) {
          break;
        }
        s2 = set2.get(k2);

      }
    }
    return pos;
  }


  protected static int unsignedOneSidedGallopingIntersect2by2(final ShortBuffer smallSet,
      final int smallLength, final ShortBuffer largeSet, final int largeLength,
      final short[] buffer) {
    if (0 == smallLength) {
      return 0;
    }
    int k1 = 0;
    int k2 = 0;
    int pos = 0;

    short s1 = largeSet.get(k1);
    short s2 = smallSet.get(k2);
    while (true) {
      if (toIntUnsigned(s1) < toIntUnsigned(s2)) {
        k1 = advanceUntil(largeSet, k1, largeLength, s2);
        if (k1 == largeLength) {
          break;
        }
        s1 = largeSet.get(k1);
      }
      if (toIntUnsigned(s2) < toIntUnsigned(s1)) {
        ++k2;
        if (k2 == smallLength) {
          break;
        }
        s2 = smallSet.get(k2);
      } else {
        // (set2.get(k2) == set1.get(k1))
        buffer[pos++] = s2;
        ++k2;
        if (k2 == smallLength) {
          break;
        }
        s2 = smallSet.get(k2);
        k1 = advanceUntil(largeSet, k1, largeLength, s2);
        if (k1 == largeLength) {
          break;
        }
        s1 = largeSet.get(k1);
      }

    }
    return pos;

  }

  protected static int unsignedUnion2by2(
          final ShortBuffer set1, final int offset1, final int length1,
          final ShortBuffer set2, final int offset2, final int length2,
          final short[] buffer) {
    if (0 == length2) {
      set1.position(offset1);
      set1.get(buffer, 0, length1);
      return length1;
    }
    if (0 == length1) {
      set2.position(offset2);
      set2.get(buffer, 0, length2);
      return length2;
    }
    int pos = 0;
    int k1 = offset1, k2 = offset2;
    short s1 = set1.get(k1);
    short s2 = set2.get(k2);
    while (true) {
      int v1 = toIntUnsigned(s1);
      int v2 = toIntUnsigned(s2);
      if (v1 < v2) {
        buffer[pos++] = s1;
        ++k1;
        if (k1 >= length1 + offset1) {
          set2.position(k2);
          set2.get(buffer, pos, length2 - k2 + offset2);
          return pos + length2 - k2 + offset2;
        }
        s1 = set1.get(k1);
      } else if (v1 == v2) {
        buffer[pos++] = s1;
        ++k1;
        ++k2;
        if (k1 >= length1 + offset1) {
          set2.position(k2);
          set2.get(buffer, pos, length2 - k2 + offset2);
          return pos + length2 - k2 + offset2;
        }
        if (k2 >= length2 + offset2) {
          set1.position(k1);
          set1.get(buffer, pos, length1 - k1 + offset1);
          return pos + length1 - k1 + offset1;
        }
        s1 = set1.get(k1);
        s2 = set2.get(k2);
      } else {// if (set1.get(k1)>set2.get(k2))
        buffer[pos++] = s2;
        ++k2;
        if (k2 >= length2 + offset2) {
          set1.position(k1);
          set1.get(buffer, pos, length1 - k1 + offset1);
          return pos + length1 - k1 + offset1;
        }
        s2 = set2.get(k2);
      }
    }
    // return pos;
  }

  /**
   * Private constructor to prevent instantiation of utility class
   */
  private BufferUtil() {}
}
