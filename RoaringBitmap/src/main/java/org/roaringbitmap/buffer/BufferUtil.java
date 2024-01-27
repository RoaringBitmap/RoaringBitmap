/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.Util;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

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
  public static  MappeableContainer[] addOffset(MappeableContainer source, char offsets) {
    if(source instanceof MappeableArrayContainer) {
      return addOffsetArray((MappeableArrayContainer) source, offsets);
    } else if (source instanceof MappeableBitmapContainer) {
      return addOffsetBitmap((MappeableBitmapContainer) source, offsets);
    } else if (source instanceof MappeableRunContainer) {
      return addOffsetRun((MappeableRunContainer) source, offsets);
    }
    throw new RuntimeException("unknown container type"); // never happens
  }

  private static MappeableContainer[] addOffsetArray(MappeableArrayContainer source,
                                                     char offsets) {
    int splitIndex;
    if (source.first() + offsets > 0xFFFF) {
      splitIndex = 0;
    } else if (source.last() + offsets < 0xFFFF) {
      splitIndex = source.cardinality;
    } else {
      splitIndex = BufferUtil.unsignedBinarySearch(source.content, 0, source.cardinality,
          (char) (0x10000 - offsets));
      if (splitIndex < 0) {
        splitIndex = -splitIndex - 1;
      }
    }
    MappeableArrayContainer low = splitIndex == 0
        ? new MappeableArrayContainer()
        : new MappeableArrayContainer(splitIndex);
    MappeableArrayContainer high = source.cardinality - splitIndex == 0
        ? new MappeableArrayContainer()
        : new MappeableArrayContainer(source.cardinality - splitIndex);

    int lowCardinality = 0;
    for (int k = 0; k < splitIndex; k++) {
      int val = source.content.get(k) + offsets;
      low.content.put(lowCardinality++, (char) val);
    }
    low.cardinality = lowCardinality;

    int highCardinality = 0;
    for (int k = splitIndex; k < source.cardinality; k++) {
      int val = source.content.get(k) + offsets;
      high.content.put(highCardinality++, (char) val);
    }
    high.cardinality = highCardinality;

    return new MappeableContainer[]{low, high};
  }

  private static MappeableContainer[] addOffsetBitmap(MappeableBitmapContainer source,
                                                      char offsets) {
    MappeableBitmapContainer c = source;
    MappeableBitmapContainer low = new MappeableBitmapContainer();
    MappeableBitmapContainer high = new MappeableBitmapContainer();
    low.cardinality = -1;
    high.cardinality = -1;
    final int b = (int) offsets >>> 6;
    final int i = (int) offsets % 64;
    if (i == 0) {
      for (int k = 0; k < 1024 - b; k++) {
        low.bitmap.put(b + k, c.bitmap.get(k));
      }
      for (int k = 1024 - b; k < 1024; k++) {
        high.bitmap.put(k - (1024 - b), c.bitmap.get(k));
      }
    } else {
      //noinspection PointlessArithmeticExpression
      low.bitmap.put(b + 0, c.bitmap.get(0) << i);
      for (int k = 1; k < 1024 - b; k++) {
        low.bitmap.put(b + k, (c.bitmap.get(k) << i)
            | (c.bitmap.get(k - 1) >>> (64 - i)));
      }
      for (int k = 1024 - b; k < 1024; k++) {
        high.bitmap.put(k - (1024 - b),
            (c.bitmap.get(k) << i)
                | (c.bitmap.get(k - 1) >>> (64 - i)));
      }
      high.bitmap.put(b, (c.bitmap.get(1024 - 1) >>> (64 - i)));
    }
    return new MappeableContainer[]{low.repairAfterLazy(), high.repairAfterLazy()};
  }

  private static MappeableContainer[] addOffsetRun(MappeableRunContainer source, char offsets) {
    MappeableRunContainer c = source;
    MappeableRunContainer low = new MappeableRunContainer();
    MappeableRunContainer high = new MappeableRunContainer();
    for (int k = 0; k < c.nbrruns; k++) {
      int val = c.getValue(k);
      val += offsets;
      int finalval = val + c.getLength(k);
      if (val <= 0xFFFF) {
        if (finalval <= 0xFFFF) {
          low.smartAppend((char) val, c.getLength(k));
        } else {
          low.smartAppend((char) val, (char) (0xFFFF - val));
          high.smartAppend((char) 0, (char) finalval);
        }
      } else {
        high.smartAppend((char) val, c.getLength(k));
      }
    }
    return new MappeableContainer[]{low, high};
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
  protected static int advanceUntil(CharBuffer array, int pos, int length, char min) {
    int lower = pos + 1;

    // special handling for a possibly common sequential case
    if (lower >= length || (array.get(lower)) >= (min)) {
      return lower;
    }

    int spansize = 1; // could set larger
    // bootstrap an upper limit

    while (lower + spansize < length
        && (array.get(lower + spansize)) < (min)) {
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

    if ((array.get(upper)) < (min)) {// means
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
      char arraymid = array.get(mid);
      if (arraymid == min) {
        return mid;
      } else if ((arraymid) < (min)) {
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
  public static int iterateUntil(CharBuffer array, int pos, int length, int min) {
    while (pos < length && (array.get(pos)) < min) {
      pos++;
    }
    return pos;
  }


  protected static void arraycopy(CharBuffer src, int srcPos, CharBuffer dest, int destPos,
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

  protected static int branchyUnsignedBinarySearch(final CharBuffer array, final int begin,
      final int end, final char k) {
    // next line accelerates the possibly common case where the value would be inserted at the end
    if ((end > 0) && ((array.get(end - 1)) < (int) (k))) {
      return -end - 1;
    }
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = (array.get(middleIndex));

      if (middleValue < (int) (k)) {
        low = middleIndex + 1;
      } else if (middleValue > (int) (k)) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    return -(low + 1);
  }


  protected static int branchyUnsignedBinarySearch(final ByteBuffer array, int position,
        final int begin, final int end, final char k) {
    // next line accelerates the possibly common case where the value would be inserted at the end
    if ((end > 0) && ((array.getChar(position + (end - 1)*2)) < (int) (k))) {
      return -end - 1;
    }
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = (array.getChar(position + 2* middleIndex));

      if (middleValue < (int) (k)) {
        low = middleIndex + 1;
      } else if (middleValue > (int) (k)) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    return -(low + 1);
  }

  protected static void fillArrayAND(char[] container, LongBuffer bitmap1, LongBuffer bitmap2) {
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
          container[pos++] = (char) (k * 64 + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
      }
    } else {
      int len = bitmap1.limit();
      for (int k = 0; k < len; ++k) {
        long bitset = bitmap1.get(k) & bitmap2.get(k);
        while (bitset != 0) {
          container[pos++] = (char) (k * 64 + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
      }
    }
  }

  protected static void fillArrayANDNOT(char[] container, LongBuffer bitmap1, LongBuffer bitmap2) {
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
          container[pos++] = (char) (k * 64 + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
      }
    } else {
      int len = bitmap1.limit();
      for (int k = 0; k < len; ++k) {
        long bitset = bitmap1.get(k) & (~bitmap2.get(k));
        while (bitset != 0) {
          container[pos++] = (char) (k * 64 + numberOfTrailingZeros(bitset));
          bitset &= (bitset - 1);
        }
      }
    }
  }

  protected static void fillArrayXOR(char[] container, LongBuffer bitmap1, LongBuffer bitmap2) {
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
          container[pos++] = (char) (k * 64 + numberOfTrailingZeros(bitset));
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
      return 2 + numRuns * 2 * 2; // each run uses 2 chars, plus the initial char giving num runs
    }
    boolean isBitmap = card > MappeableArrayContainer.DEFAULT_MAX_SIZE;
    if (isBitmap) {
      return MappeableBitmapContainer.MAX_CAPACITY / 8;
    } else {
      return card * 2;
    }

  }

  protected static char highbits(int x) {
    return (char) (x >>> 16);
  }


  protected static char highbits(long x) {
    return (char) (x >>> 16);
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

  protected static char lowbits(int x) {
    return (char) x;
  }

  protected static char lowbits(long x) {
    return (char) x;
  }

  protected static int lowbitsAsInteger(long x) {
    return (int)(x & 0xFFFF);
  }

  protected static char maxLowBit() {
    return (char) 0xFFFF;
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

  /**
   * Look for value k in buffer in the range [begin,end). If the value is found, return its index.
   * If not, return -(i+1) where i is the index where the value would be inserted. The buffer is
   * assumed to contain sorted values where chars are interpreted as unsigned integers.
   *
   * @param array buffer where we search
   * @param begin first index (inclusive)
   * @param end last index (exclusive)
   * @param k value we search for
   * @return count
   */
  public static int unsignedBinarySearch(final CharBuffer array, final int begin, final int end,
                                         final char k) {
    return branchyUnsignedBinarySearch(array, begin, end, k);
  }
  /**
   * Look for value k in buffer in the range [begin,end). If the value is found, return its index.
   * If not, return -(i+1) where i is the index where the value would be inserted. The buffer is
   * assumed to contain sorted values where chars are interpreted as unsigned integers.
   *
   * @param array buffer where we search
   * @param position starting position of the container in the ByteBuffer
   * @param begin first index (inclusive)
   * @param end last index (exclusive)
   * @param k value we search for
   * @return count
   */
  public static int unsignedBinarySearch(final ByteBuffer array, int position,
      final int begin, final int end, final char k) {
    return branchyUnsignedBinarySearch(array, position, begin, end, k);
  }

  protected static int unsignedDifference(final CharBuffer set1, final int length1,
      final CharBuffer set2, final int length2, final char[] buffer) {
    int pos = 0;
    int k1 = 0, k2 = 0;
    if (0 == length2) {
      set1.get(buffer, 0, length1);
      return length1;
    }
    if (0 == length1) {
      return 0;
    }
    char s1 = set1.get(k1);
    char s2 = set2.get(k2);
    while (true) {
      if (s1 < s2) {
        buffer[pos++] = s1;
        ++k1;
        if (k1 >= length1) {
          break;
        }
        s1 = set1.get(k1);
      } else if (s1 == s2) {
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


  /**
   * Intersects the bitmap with the array, returning the cardinality of the result
   * @param bitmap the bitmap, modified
   * @param array the array, not modified
   * @param length how much of the array to consume
   * @return the size of the intersection, i.e. how many bits still set in the bitmap
   */
  public static int intersectArrayIntoBitmap(long[] bitmap, CharBuffer array, int length) {
    int lastWordIndex = 0;
    int wordIndex = 0;
    long word = 0L;
    int cardinality = 0;
    for (int i = 0; i < length; ++i) {
      wordIndex = array.get(i) >>> 6;
      if (wordIndex != lastWordIndex) {
        bitmap[lastWordIndex] &= word;
        cardinality += Long.bitCount(bitmap[lastWordIndex]);
        word = 0L;
        Arrays.fill(bitmap, lastWordIndex + 1, wordIndex, 0L);
        lastWordIndex = wordIndex;
      }
      word |= 1L << array.get(i);
    }
    if (word != 0L) {
      bitmap[wordIndex] &= word;
      cardinality += Long.bitCount(bitmap[lastWordIndex]);
    }
    if (wordIndex < bitmap.length) {
      Arrays.fill(bitmap, wordIndex + 1, bitmap.length, 0L);
    }
    return cardinality;
  }

  /**
   * Intersects the bitmap with the array, returning the cardinality of the result
   * @param bitmap the bitmap, modified
   * @param array the array, not modified
   * @param length how much of the array to consume
   * @return the size of the intersection, i.e. how many bits still set in the bitmap
   */
  public static int intersectArrayIntoBitmap(LongBuffer bitmap, CharBuffer array, int length) {
    if (isBackedBySimpleArray(bitmap)) {
      return intersectArrayIntoBitmap(bitmap.array(), array, length);
    }
    int lastWordIndex = 0;
    int wordIndex = 0;
    long word = 0L;
    int cardinality = 0;
    for (int i = 0; i < length; ++i) {
      wordIndex = array.get(i) >>> 6;
      if (wordIndex != lastWordIndex) {
        long lastWord = bitmap.get(lastWordIndex);
        lastWord &= word;
        bitmap.put(lastWordIndex, lastWord);
        cardinality += Long.bitCount(lastWord);
        word = 0L;
        for (int j = lastWordIndex + 1; j < wordIndex; ++j) {
          bitmap.put(j, 0L);
        }
        lastWordIndex = wordIndex;
      }
      word |= 1L << array.get(i);
    }
    if (word != 0L) {
      long currentWord = bitmap.get(wordIndex);
      currentWord &= word;
      bitmap.put(wordIndex, currentWord);
      cardinality += Long.bitCount(currentWord);
    }
    if (wordIndex < bitmap.limit()) {
      for (int j = wordIndex + 1; j < bitmap.limit(); ++j) {
        bitmap.put(j, 0L);
      }
    }
    return cardinality;
  }

  protected static int unsignedExclusiveUnion2by2(final CharBuffer set1, final int length1,
      final CharBuffer set2, final int length2, final char[] buffer) {
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
    char s1 = set1.get(k1);
    char s2 = set2.get(k2);
    while (true) {
      if (s1 < s2) {
        buffer[pos++] = s1;
        ++k1;
        if (k1 >= length1) {
          set2.position(k2);
          set2.get(buffer, pos, length2 - k2);
          return pos + length2 - k2;
        }
        s1 = set1.get(k1);
      } else if (s1 == s2) {
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


  protected static int unsignedIntersect2by2(final CharBuffer set1, final int length1,
      final CharBuffer set2, final int length2, final char[] buffer) {
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
  public static boolean unsignedIntersects(CharBuffer set1, int length1, CharBuffer set2,
      int length2) {
    if ((0 == length1) || (0 == length2)) {
      return false;
    }
    int k1 = 0;
    int k2 = 0;

    // could be more efficient with galloping
    char s1 = set1.get(k1);
    char s2 = set2.get(k2);

    mainwhile: while (true) {
      if (s2 < s1) {
        do {
          ++k2;
          if (k2 == length2) {
            break mainwhile;
          }
          s2 = set2.get(k2);
        } while (s2 < s1);
      }
      if (s1 < s2) {
        do {
          ++k1;
          if (k1 == length1) {
            break mainwhile;
          }
          s1 = set1.get(k1);
        } while (s1 < s2);
      } else {
        return true;
      }
    }
    return false;
  }

  protected static int unsignedLocalIntersect2by2(final CharBuffer set1, final int length1,
      final CharBuffer set2, final int length2, final char[] buffer) {
    if ((0 == length1) || (0 == length2)) {
      return 0;
    }
    int k1 = 0;
    int k2 = 0;
    int pos = 0;
    char s1 = set1.get(k1);
    char s2 = set2.get(k2);

    mainwhile: while (true) {
      if (s2 < s1) {
        do {
          ++k2;
          if (k2 == length2) {
            break mainwhile;
          }
          s2 = set2.get(k2);

        } while (s2 < s1);
      }
      if (s1 < s2) {
        do {
          ++k1;
          if (k1 == length1) {
            break mainwhile;
          }
          s1 = set1.get(k1);

        } while (s1 < s2);
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

  protected static int unsignedLocalIntersect2by2Cardinality(final CharBuffer set1,
      final int length1, final CharBuffer set2, final int length2) {
    if ((0 == length1) || (0 == length2)) {
      return 0;
    }
    int k1 = 0;
    int k2 = 0;
    int pos = 0;
    char s1 = set1.get(k1);
    char s2 = set2.get(k2);

    mainwhile: while (true) {
      if (s2 < s1) {
        do {
          ++k2;
          if (k2 == length2) {
            break mainwhile;
          }
          s2 = set2.get(k2);

        } while (s2 < s1);
      }
      if (s1 < s2) {
        do {
          ++k1;
          if (k1 == length1) {
            break mainwhile;
          }
          s1 = set1.get(k1);

        } while (s1 < s2);
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


  protected static int unsignedOneSidedGallopingIntersect2by2(final CharBuffer smallSet,
      final int smallLength, final CharBuffer largeSet, final int largeLength,
      final char[] buffer) {
    if (0 == smallLength) {
      return 0;
    }
    int k1 = 0;
    int k2 = 0;
    int pos = 0;

    char s1 = largeSet.get(k1);
    char s2 = smallSet.get(k2);
    while (true) {
      if (s1 < s2) {
        k1 = advanceUntil(largeSet, k1, largeLength, s2);
        if (k1 == largeLength) {
          break;
        }
        s1 = largeSet.get(k1);
      }
      if (s2 < s1) {
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
          final CharBuffer set1, final int offset1, final int length1,
          final CharBuffer set2, final int offset2, final int length2,
          final char[] buffer) {
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
    char s1 = set1.get(k1);
    char s2 = set2.get(k2);
    while (true) {
      int v1 = s1;
      int v2 = s2;
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
  private BufferUtil() {

  }
}
