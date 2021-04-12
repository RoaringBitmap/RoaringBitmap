/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.util.Arrays;

import static java.lang.Long.numberOfTrailingZeros;

/**
 * Various useful methods for roaring bitmaps.
 */
public final class Util {

  /**
   * optimization flag: whether to use hybrid binary search: hybrid formats
   * combine a binary search with a sequential search
   */
  public static final boolean USE_HYBRID_BINSEARCH = true;


  /**
   * Add value "offset" to all values in the container, producing
   * two new containers. The existing container remains unchanged.
   * The new container are not converted, so they need to be checked:
   * e.g., we could produce two bitmap containers having low cardinality.
   * @param source source container
   * @param offsets value to add to each value in the container
   * @return return an array made of two containers
   */
  public static  Container[] addOffset(Container source, char offsets) {
    // could be a whole lot faster, this is a simple implementation
    if(source instanceof ArrayContainer) {
      ArrayContainer c = (ArrayContainer) source;
      ArrayContainer low = new ArrayContainer(c.cardinality);
      ArrayContainer high = new ArrayContainer(c.cardinality);
      for(int k = 0; k < c.cardinality; k++) {
        int val =  c.content[k];
        val += (int) (offsets);
        if(val <= 0xFFFF) {
          low.content[low.cardinality++] = (char) val;
        } else {
          high.content[high.cardinality++] = (char) val;
        }
      }
      return new Container[] {low, high};
    } else if (source instanceof BitmapContainer) {
      BitmapContainer c = (BitmapContainer) source;
      BitmapContainer low = new BitmapContainer();
      BitmapContainer high = new BitmapContainer();
      low.cardinality = -1;
      high.cardinality = -1;
      final int b = (int) (offsets) >>> 6;
      final int i = (int) (offsets) % 64;
      if(i == 0) {
        System.arraycopy(c.bitmap, 0, low.bitmap, b, 1024 - b);
        System.arraycopy(c.bitmap, 1024 - b, high.bitmap, 0, b );
      } else {
        low.bitmap[b + 0] = c.bitmap[0] << i;
        for(int k = 1; k < 1024 - b; k++) {
          low.bitmap[b + k] = (c.bitmap[k] << i) | (c.bitmap[k - 1] >>> (64-i));
        }
        for(int k = 1024 - b; k < 1024 ; k++) {
          high.bitmap[k - (1024 - b)] =
             (c.bitmap[k] << i)
             | (c.bitmap[k - 1] >>> (64-i));
        }
        high.bitmap[b] =  (c.bitmap[1024 - 1] >>> (64-i));
      }
      return new Container[] {low.repairAfterLazy(), high.repairAfterLazy()};
    } else if (source instanceof RunContainer) {
      RunContainer input = (RunContainer) source;
      RunContainer low = new RunContainer();
      RunContainer high = new RunContainer();
      for(int k = 0 ; k < input.nbrruns; k++) {
        int val =  (input.getValue(k));
        val += (int) (offsets);
        int finalval =  val + (input.getLength(k));
        if(val <= 0xFFFF) {
          if(finalval <= 0xFFFF) {
            low.smartAppend((char)val,input.getLength(k));
          } else {
            low.smartAppend((char)val,(char)(0xFFFF-val));
            high.smartAppend((char) 0,(char)finalval);
          }
        } else {
          high.smartAppend((char)val,input.getLength(k));
        }
      }
      return new Container[] {low, high};
    }
    throw new RuntimeException("unknown container type"); // never happens
  }

  /**
   * Find the smallest integer larger than pos such that array[pos]&gt;= min. If none can be found,
   * return length. Based on code by O. Kaser.
   *
   * @param array array to search within
   * @param pos starting position of the search
   * @param length length of the array to search
   * @param min minimum value
   * @return x greater than pos such that array[pos] is at least as large as min, pos is is equal to
   *         length if it is not possible.
   */
  public static int advanceUntil(char[] array, int pos, int length, char min) {
    int lower = pos + 1;

    // special handling for a possibly common sequential case
    if (lower >= length || (array[lower]) >= (int) (min)) {
      return lower;
    }

    int spansize = 1; // could set larger
    // bootstrap an upper limit

    while (lower + spansize < length
        && (array[lower + spansize]) < (int) (min)) {
      spansize *= 2; // hoping for compiler will reduce to
    }
    // shift
    int upper = (lower + spansize < length) ? lower + spansize : length - 1;

    // maybe we are lucky (could be common case when the seek ahead
    // expected
    // to be small and sequential will otherwise make us look bad)
    if (array[upper] == min) {
      return upper;
    }

    if ((array[upper]) < (int) (min)) {
      // means array has no item >= min pos = array.length;
      return length;
    }

    // we know that the next-smallest span was too small
    lower += (spansize >>> 1);

    // else begin binary search
    // invariant: array[lower]<min && array[upper]>min
    while (lower + 1 != upper) {
      int mid = (lower + upper) >>> 1;
      char arraymid = array[mid];
      if (arraymid == min) {
        return mid;
      } else if ((arraymid) < (int) (min)) {
        lower = mid;
      } else {
        upper = mid;
      }
    }
    return upper;

  }
  
  
  /**
   * Find the largest integer smaller than pos such that array[pos]&lt;= max. If none can be found,
   * return length. Based on code by O. Kaser.
   *
   * @param array array to search within
   * @param pos starting position of the search
   * @param length length of the array to search
   * @param max maximum value
   * @return x less than pos such that array[pos] is at least as small as max, pos is is equal to
   *         0 if it is not possible.
   */
  public static int reverseUntil(char[] array, int pos, int length, char max) {
    int lower = pos - 1;

    // special handling for a possibly common sequential case
    if (lower <= 0 || (array[lower]) <= (int) (max)) {
      return lower;
    }

    int spansize = 1; // could set larger
    // bootstrap an upper limit

    while (lower - spansize > 0
        && (array[lower - spansize]) > (int) (max)) {
      spansize *= 2; // hoping for compiler will reduce to
    }
    // shift
    int upper = (lower - spansize > 0) ? lower - spansize : 0;

    // maybe we are lucky (could be common case when the seek ahead
    // expected
    // to be small and sequential will otherwise make us look bad)
    if (array[upper] == max) {
      return upper;
    }

    if ((array[upper]) > (int) (max)) {
      // means array has no item >= min pos = array.length;
      return 0;
    }

    // we know that the next-smallest span was too small
    lower -= (spansize >>> 1);

    // else begin binary search
    // invariant: array[lower]<min && array[upper]>min
    while (lower - 1 != upper) {
      int mid = (lower + upper) >>> 1;
      char arraymid = array[mid];
      if (arraymid == max) {
        return mid;
      } else if ((arraymid) > (int) (max)) {
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
  public static int iterateUntil(char[] array, int pos, int length, int min) {
    while (pos < length && (array[pos]) < min) {
      pos++;
    }
    return pos;
  }

  protected static int branchyUnsignedBinarySearch(final char[] array, final int begin,
      final int end, final char k) {
    // next line accelerates the possibly common case where the value would
    // be inserted at the end
    if ((end > 0) && ((array[end - 1]) < (int) (k))) {
      return -end - 1;
    }
    int low = begin;
    int high = end - 1;
    while (low <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = (array[middleIndex]);

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

  /**
   * Compute the bitwise AND between two long arrays and write the set bits in the container.
   *
   * @param container where we write
   * @param bitmap1 first bitmap
   * @param bitmap2 second bitmap
   */
  public static void fillArrayAND(final char[] container, final long[] bitmap1,
      final long[] bitmap2) {
    int pos = 0;
    if (bitmap1.length != bitmap2.length) {
      throw new IllegalArgumentException("not supported");
    }
    for (int k = 0; k < bitmap1.length; ++k) {
      long bitset = bitmap1[k] & bitmap2[k];
      while (bitset != 0) {
        container[pos++] = (char) (k * 64 + numberOfTrailingZeros(bitset));
        bitset &= (bitset - 1);
      }
    }
  }

  /**
   * Compute the bitwise ANDNOT between two long arrays and write the set bits in the container.
   *
   * @param container where we write
   * @param bitmap1 first bitmap
   * @param bitmap2 second bitmap
   */
  public static void fillArrayANDNOT(final char[] container, final long[] bitmap1,
      final long[] bitmap2) {
    int pos = 0;
    if (bitmap1.length != bitmap2.length) {
      throw new IllegalArgumentException("not supported");
    }
    for (int k = 0; k < bitmap1.length; ++k) {
      long bitset = bitmap1[k] & (~bitmap2[k]);
      while (bitset != 0) {
        container[pos++] = (char) (k * 64 + numberOfTrailingZeros(bitset));
        bitset &= (bitset - 1);
      }
    }
  }

  /**
   * Compute the bitwise XOR between two long arrays and write the set bits in the container.
   *
   * @param container where we write
   * @param bitmap1 first bitmap
   * @param bitmap2 second bitmap
   */
  public static void fillArrayXOR(final char[] container, final long[] bitmap1,
      final long[] bitmap2) {
    int pos = 0;
    if (bitmap1.length != bitmap2.length) {
      throw new IllegalArgumentException("not supported");
    }
    for (int k = 0; k < bitmap1.length; ++k) {
      long bitset = bitmap1[k] ^ bitmap2[k];
      while (bitset != 0) {
        container[pos++] = (char) (k * 64 + numberOfTrailingZeros(bitset));
        bitset &= (bitset - 1);
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
  public static void flipBitmapRange(long[] bitmap, int start, int end) {
    if (start == end) {
      return;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;
    bitmap[firstword] ^= ~(~0L << start);
    for (int i = firstword; i < endword; i++) {
      bitmap[i] = ~bitmap[i];
    }
    bitmap[endword] ^= ~0L >>> -end;
  }


  /**
   * Hamming weight of the 64-bit words involved in the range
   *  start, start+1,..., end-1, that is, it will compute the
   * cardinality of the bitset from index (floor(start/64) to floor((end-1)/64))
   * inclusively.
   *
   * @param bitmap array of words representing a bitset
   * @param start first index  (inclusive)
   * @param end last index (exclusive)
   * @return the hamming weight of the corresponding words
   */
  @Deprecated
  public static int cardinalityInBitmapWordRange(long[] bitmap, int start, int end) {
    if (start >= end) {
      return 0;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;
    int answer = 0;
    for (int i = firstword; i <= endword; i++) {
      answer += Long.bitCount(bitmap[i]);
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
  public static int cardinalityInBitmapRange(long[] bitmap, int start, int end) {
    if (start >= end) {
      return 0;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;
    if (firstword == endword) {
      return Long.bitCount(bitmap[firstword] & ( (~0L << start) & (~0L >>> -end) ));
    }
    int answer = Long.bitCount(bitmap[firstword] & (~0L << start));
    for (int i = firstword + 1; i < endword; i++) {
      answer += Long.bitCount(bitmap[i]);
    }
    answer += Long.bitCount(bitmap[endword] & (~0L >>> -end));
    return answer;
  }

  protected static char highbits(int x) {
    return (char) (x >>> 16);
  }

  protected static char highbits(long x) {
    return (char) (x >>> 16);
  }

  // starts with binary search and finishes with a sequential search
  protected static int hybridUnsignedBinarySearch(final char[] array, final int begin,
      final int end, final char k) {
    // next line accelerates the possibly common case where the value would
    // be inserted at the end
    if ((end > 0) && ((array[end - 1]) < (int) k)) {
      return -end - 1;
    }
    int low = begin;
    int high = end - 1;
    // 32 in the next line matches the size of a cache line
    while (low + 32 <= high) {
      final int middleIndex = (low + high) >>> 1;
      final int middleValue = (array[middleIndex]);

      if (middleValue < (int) k) {
        low = middleIndex + 1;
      } else if (middleValue > (int) k) {
        high = middleIndex - 1;
      } else {
        return middleIndex;
      }
    }
    // we finish the job with a sequential search
    int x = low;
    for (; x <= high; ++x) {
      final int val = (array[x]);
      if (val >= (int) k) {
        if (val == (int) k) {
          return x;
        }
        break;
      }
    }
    return -(x + 1);
  }

  protected static char lowbits(int x) {
    return (char) x;
  }

  protected static char lowbits(long x) {
    return (char) x;
  }


  protected static int lowbitsAsInteger(int x) {
    return x & 0xFFFF;
  }

  protected static int lowbitsAsInteger(long x) {
    return (int)(x & 0xFFFF);
  }

  public static int maxLowBitAsInteger() {
    return 0xFFFF;
  }

  /**
   * clear bits at start, start+1,..., end-1
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   */
  public static void resetBitmapRange(long[] bitmap, int start, int end) {
    if (start == end) {
      return;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;

    if (firstword == endword) {
      bitmap[firstword] &= ~((~0L << start) & (~0L >>> -end));
      return;
    }
    bitmap[firstword] &= ~(~0L << start);
    for (int i = firstword + 1; i < endword; i++) {
      bitmap[i] = 0;
    }
    bitmap[endword] &= ~(~0L >>> -end);

  }

  /**
   * Intersects the bitmap with the array, returning the cardinality of the result
   * @param bitmap the bitmap, modified
   * @param array the array, not modified
   * @param length how much of the array to consume
   * @return the size of the intersection, i.e. how many bits still set in the bitmap
   */
  public static int intersectArrayIntoBitmap(long[] bitmap, char[] array, int length) {
    int lastWordIndex = 0;
    int wordIndex = 0;
    long word = 0L;
    int cardinality = 0;
    for (int i = 0; i < length; ++i) {
      wordIndex = array[i] >>> 6;
      if (wordIndex != lastWordIndex) {
        bitmap[lastWordIndex] &= word;
        cardinality += Long.bitCount(bitmap[lastWordIndex]);
        word = 0L;
        Arrays.fill(bitmap, lastWordIndex + 1, wordIndex, 0L);
        lastWordIndex = wordIndex;
      }
      word |= 1L << array[i];
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
   * Given a word w, return the position of the jth true bit.
   *
   * @param w word
   * @param j index
   * @return position of jth true bit in w
   */
  public static int select(long w, int j) {
    int seen = 0;
    // Divide 64bit
    int part = (int) w;
    int n = Integer.bitCount(part);
    if (n <= j) {
      part = (int) (w >>> 32);
      seen += 32;
      j -= n;
    }
    int ww = part;

    // Divide 32bit
    part = ww & 0xFFFF;

    n = Integer.bitCount(part);
    if (n <= j) {

      part = ww >>> 16;
      seen += 16;
      j -= n;
    }
    ww = part;

    // Divide 16bit
    part = ww & 0xFF;
    n = Integer.bitCount(part);
    if (n <= j) {
      part = ww >>> 8;
      seen += 8;
      j -= n;
    }
    ww = part;

    // Lookup in final byte
    int counter;
    for (counter = 0; counter < 8; counter++) {
      j -= (ww >>> counter) & 1;
      if (j < 0) {
        break;
      }
    }
    return seen + counter;
  }

  /**
   * set bits at start, start+1,..., end-1
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   */
  public static void setBitmapRange(long[] bitmap, int start, int end) {
    if (start == end) {
      return;
    }
    int firstword = start / 64;
    int endword = (end - 1) / 64;
    if (firstword == endword) {
      bitmap[firstword] |= (~0L << start) & (~0L >>> -end);
      return;
    }
    bitmap[firstword] |= ~0L << start;
    for (int i = firstword + 1; i < endword; i++) {
      bitmap[i] = ~0L;
    }
    bitmap[endword] |= ~0L >>> -end;
  }

  /**
   * set bits at start, start+1,..., end-1 and report the
   * cardinality change
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   * @return cardinality change
   */
  @Deprecated
  public static int setBitmapRangeAndCardinalityChange(long[] bitmap, int start, int end) {
    int cardbefore = cardinalityInBitmapWordRange(bitmap, start, end);
    setBitmapRange(bitmap, start,end);
    int cardafter = cardinalityInBitmapWordRange(bitmap, start, end);
    return cardafter - cardbefore;
  }


  /**
   * flip  bits at start, start+1,..., end-1 and report the
   * cardinality change
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   * @return cardinality change
   */
  @Deprecated
  public static int flipBitmapRangeAndCardinalityChange(long[] bitmap, int start, int end) {
    int cardbefore = cardinalityInBitmapWordRange(bitmap, start, end);
    flipBitmapRange(bitmap, start,end);
    int cardafter = cardinalityInBitmapWordRange(bitmap, start, end);
    return cardafter - cardbefore;
  }


  /**
   * reset  bits at start, start+1,..., end-1 and report the
   * cardinality change
   *
   * @param bitmap array of words to be modified
   * @param start first index to be modified (inclusive)
   * @param end last index to be modified (exclusive)
   * @return cardinality change
   */
  @Deprecated
  public static int resetBitmapRangeAndCardinalityChange(long[] bitmap, int start, int end) {
    int cardbefore = cardinalityInBitmapWordRange(bitmap, start, end);
    resetBitmapRange(bitmap, start,end);
    int cardafter = cardinalityInBitmapWordRange(bitmap, start, end);
    return cardafter - cardbefore;
  }

  /**
   * Look for value k in array in the range [begin,end). If the value is found, return its index. If
   * not, return -(i+1) where i is the index where the value would be inserted. The array is assumed
   * to contain sorted values where shorts are interpreted as unsigned integers.
   *
   * @param array array where we search
   * @param begin first index (inclusive)
   * @param end last index (exclusive)
   * @param k value we search for
   * @return count
   */
  public static int unsignedBinarySearch(final char[] array, final int begin, final int end,
      final char k) {
    if (USE_HYBRID_BINSEARCH) {
      return hybridUnsignedBinarySearch(array, begin, end, k);
    } else {
      return branchyUnsignedBinarySearch(array, begin, end, k);
    }
  }

  /**
   * Compute the difference between two sorted lists and write the result to the provided output
   * array
   *
   * @param set1 first array
   * @param length1 length of first array
   * @param set2 second array
   * @param length2 length of second array
   * @param buffer output array
   * @return cardinality of the difference
   */
  public static int unsignedDifference(final char[] set1, final int length1, final char[] set2,
      final int length2, final char[] buffer) {
    int pos = 0;
    int k1 = 0, k2 = 0;
    if (0 == length2) {
      System.arraycopy(set1, 0, buffer, 0, length1);
      return length1;
    }
    if (0 == length1) {
      return 0;
    }
    char s1 = set1[k1];
    char s2 = set2[k2];
    while (true) {
      if (s1 < s2) {
        buffer[pos++] = s1;
        ++k1;
        if (k1 >= length1) {
          break;
        }
        s1 = set1[k1];
      } else if (s1 == s2) {
        ++k1;
        ++k2;
        if (k1 >= length1) {
          break;
        }
        if (k2 >= length2) {
          System.arraycopy(set1, k1, buffer, pos, length1 - k1);
          return pos + length1 - k1;
        }
        s1 = set1[k1];
        s2 = set2[k2];
      } else {// if (val1>val2)
        ++k2;
        if (k2 >= length2) {
          System.arraycopy(set1, k1, buffer, pos, length1 - k1);
          return pos + length1 - k1;
        }
        s2 = set2[k2];
      }
    }
    return pos;
  }

  /**
   * Compute the difference between two sorted lists and write the result to the provided output
   * array
   *
   * @param set1 first array
   * @param set2 second array
   * @param buffer output array
   * @return cardinality of the difference
   */
  public static int unsignedDifference(CharIterator set1, CharIterator set2,
                                       final char[] buffer) {
    int pos = 0;
    if (!set2.hasNext()) {
      while (set1.hasNext()) {
        buffer[pos++] = set1.next();
      }
      return pos;
    }
    if (!set1.hasNext()) {
      return 0;
    }
    char v1 = set1.next();
    char v2 = set2.next();
    while (true) {
      if ((v1) < (v2)) {
        buffer[pos++] = v1;
        if (!set1.hasNext()) {
          return pos;
        }
        v1 = set1.next();
      } else if (v1 == v2) {
        if (!set1.hasNext()) {
          break;
        }
        if (!set2.hasNext()) {
          while (set1.hasNext()) {
            buffer[pos++] = set1.next();
          }
          return pos;
        }
        v1 = set1.next();
        v2 = set2.next();
      } else {// if (val1>val2)
        if (!set2.hasNext()) {
          buffer[pos++] = v1;
          while (set1.hasNext()) {
            buffer[pos++] = set1.next();
          }
          return pos;
        }
        v2 = set2.next();
      }
    }
    return pos;
  }

  /**
   * Compute the exclusive union of two sorted lists and write the result to the provided output
   * array
   *
   * @param set1 first array
   * @param length1 length of first array
   * @param set2 second array
   * @param length2 length of second array
   * @param buffer output array
   * @return cardinality of the exclusive union
   */
  public static int unsignedExclusiveUnion2by2(final char[] set1, final int length1,
      final char[] set2, final int length2, final char[] buffer) {
    int pos = 0;
    int k1 = 0, k2 = 0;
    if (0 == length2) {
      System.arraycopy(set1, 0, buffer, 0, length1);
      return length1;
    }
    if (0 == length1) {
      System.arraycopy(set2, 0, buffer, 0, length2);
      return length2;
    }
    char s1 = set1[k1];
    char s2 = set2[k2];
    while (true) {
      if (s1 < s2) {
        buffer[pos++] = s1;
        ++k1;
        if (k1 >= length1) {
          System.arraycopy(set2, k2, buffer, pos, length2 - k2);
          return pos + length2 - k2;
        }
        s1 = set1[k1];
      } else if (s1 == s2) {
        ++k1;
        ++k2;
        if (k1 >= length1) {
          System.arraycopy(set2, k2, buffer, pos, length2 - k2);
          return pos + length2 - k2;
        }
        if (k2 >= length2) {
          System.arraycopy(set1, k1, buffer, pos, length1 - k1);
          return pos + length1 - k1;
        }
        s1 = set1[k1];
        s2 = set2[k2];
      } else {// if (val1>val2)
        buffer[pos++] = s2;
        ++k2;
        if (k2 >= length2) {
          System.arraycopy(set1, k1, buffer, pos, length1 - k1);
          return pos + length1 - k1;
        }
        s2 = set2[k2];
      }
    }
    // return pos;
  }



  /**
   * Intersect two sorted lists and write the result to the provided output array
   *
   * @param set1 first array
   * @param length1 length of first array
   * @param set2 second array
   * @param length2 length of second array
   * @param buffer output array
   * @return cardinality of the intersection
   */
  public static int unsignedIntersect2by2(final char[] set1, final int length1, final char[] set2,
      final int length2, final char[] buffer) {
    final int THRESHOLD = 25;
    if (set1.length * THRESHOLD < set2.length) {
      return unsignedOneSidedGallopingIntersect2by2(set1, length1, set2, length2, buffer);
    } else if (set2.length * THRESHOLD < set1.length) {
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
  public static boolean unsignedIntersects(char[] set1, int length1, char[] set2, int length2) {
    // galloping might be faster, but we do not expect this function to be slow
    if ((0 == length1) || (0 == length2)) {
      return false;
    }
    int k1 = 0;
    int k2 = 0;
    char s1 = set1[k1];
    char s2 = set2[k2];
    mainwhile: while (true) {
      if (s2 < s1) {
        do {
          ++k2;
          if (k2 == length2) {
            break mainwhile;
          }
          s2 = set2[k2];
        } while (s2 < s1);
      }
      if (s1 < s2) {
        do {
          ++k1;
          if (k1 == length1) {
            break mainwhile;
          }
          s1 = set1[k1];
        } while (s1 < s2);
      } else {
        return true;
      }
    }
    return false;
  }


  protected static int unsignedLocalIntersect2by2(final char[] set1, final int length1,
      final char[] set2, final int length2, final char[] buffer) {
    if ((0 == length1) || (0 == length2)) {
      return 0;
    }
    int k1 = 0;
    int k2 = 0;
    int pos = 0;
    char s1 = set1[k1];
    char s2 = set2[k2];

    mainwhile: while (true) {
      int v1 = (s1);
      int v2 = s2;
      if (v2 < v1) {
        do {
          ++k2;
          if (k2 == length2) {
            break mainwhile;
          }
          s2 = set2[k2];
          v2 = s2;
        } while (v2 < v1);
      }
      if (v1 < v2) {
        do {
          ++k1;
          if (k1 == length1) {
            break mainwhile;
          }
          s1 = set1[k1];
          v1 = s1;
        } while (v1 < v2);
      } else {
        // (set2[k2] == set1[k1])
        buffer[pos++] = s1;
        ++k1;
        if (k1 == length1) {
          break;
        }
        ++k2;
        if (k2 == length2) {
          break;
        }
        s1 = set1[k1];
        s2 = set2[k2];
      }
    }
    return pos;
  }


  /**
   * Compute the cardinality of the intersection
   * @param set1 first set
   * @param length1 how many values to consider in the first set
   * @param set2 second set
   * @param length2 how many values to consider in the second set
   * @return cardinality of the intersection
   */
  public static int unsignedLocalIntersect2by2Cardinality(final char[] set1, final int length1,
      final char[] set2, final int length2) {
    if ((0 == length1) || (0 == length2)) {
      return 0;
    }
    int k1 = 0;
    int k2 = 0;
    int pos = 0;
    char s1 = set1[k1];
    char s2 = set2[k2];

    mainwhile: while (true) {
      int v1 = s1;
      int v2 = s2;
      if (v2 < v1) {
        do {
          ++k2;
          if (k2 == length2) {
            break mainwhile;
          }
          s2 = set2[k2];
          v2 = s2;
        } while (v2 < v1);
      }
      if (v1 < v2) {
        do {
          ++k1;
          if (k1 == length1) {
            break mainwhile;
          }
          s1 = set1[k1];
          v1 = s1;
        } while (v1 < v2);
      } else {
        // (set2[k2] == set1[k1])
        pos++;
        ++k1;
        if (k1 == length1) {
          break;
        }
        ++k2;
        if (k2 == length2) {
          break;
        }
        s1 = set1[k1];
        s2 = set2[k2];
      }
    }
    return pos;
  }


  protected static int unsignedOneSidedGallopingIntersect2by2(final char[] smallSet,
      final int smallLength, final char[] largeSet, final int largeLength, final char[] buffer) {
    if (0 == smallLength) {
      return 0;
    }
    int k1 = 0;
    int k2 = 0;
    int pos = 0;
    char s1 = largeSet[k1];
    char s2 = smallSet[k2];
    while (true) {
      if (s1 < s2) {
        k1 = advanceUntil(largeSet, k1, largeLength, s2);
        if (k1 == largeLength) {
          break;
        }
        s1 = largeSet[k1];
      }
      if (s2 < s1) {
        ++k2;
        if (k2 == smallLength) {
          break;
        }
        s2 = smallSet[k2];
      } else {
        // (set2[k2] == set1[k1])
        buffer[pos++] = s2;
        ++k2;
        if (k2 == smallLength) {
          break;
        }
        s2 = smallSet[k2];
        k1 = advanceUntil(largeSet, k1, largeLength, s2);
        if (k1 == largeLength) {
          break;
        }
        s1 = largeSet[k1];
      }

    }
    return pos;

  }

  /**
   * Unite two sorted lists and write the result to the provided output array
   *
   * @param set1 first array
   * @param offset1 offset of first array
   * @param length1 length of first array
   * @param set2 second array
   * @param offset2 offset of second array
   * @param length2 length of second array
   * @param buffer output array
   * @return cardinality of the union
   */
  public static int unsignedUnion2by2(
          final char[] set1, final int offset1, final int length1,
          final char[] set2, final int offset2, final int length2,
          final char[] buffer) {
    if (0 == length2) {
      System.arraycopy(set1, offset1, buffer, 0, length1);
      return length1;
    }
    if (0 == length1) {
      System.arraycopy(set2, offset2, buffer, 0, length2);
      return length2;
    }
    int pos = 0;
    int k1 = offset1, k2 = offset2;
    char s1 = set1[k1];
    char s2 = set2[k2];
    while (true) {
      int v1 = s1;
      int v2 = s2;
      if (v1 < v2) {
        buffer[pos++] = s1;
        ++k1;
        if (k1 >= length1 + offset1) {
          System.arraycopy(set2, k2, buffer, pos, length2 - k2 + offset2);
          return pos + length2 - k2 + offset2;
        }
        s1 = set1[k1];
      } else if (v1 == v2) {
        buffer[pos++] = s1;
        ++k1;
        ++k2;
        if (k1 >= length1 + offset1) {
          System.arraycopy(set2, k2, buffer, pos, length2 - k2 + offset2);
          return pos + length2 - k2 + offset2;
        }
        if (k2 >= length2 + offset2) {
          System.arraycopy(set1, k1, buffer, pos, length1 - k1 + offset1);
          return pos + length1 - k1 + offset1;
        }
        s1 = set1[k1];
        s2 = set2[k2];
      } else {// if (set1[k1]>set2[k2])
        buffer[pos++] = s2;
        ++k2;
        if (k2 >= length2 + offset2) {
          System.arraycopy(set1, k1, buffer, pos, length1 - k1 + offset1);
          return pos + length1 - k1 + offset1;
        }
        s2 = set2[k2];
      }
    }
    // return pos;
  }


  /**
   * Converts the argument to a {@code long} by an unsigned conversion. In an unsigned conversion to
   * a {@code long}, the high-order 32 bits of the {@code long} are zero and the low-order 32 bits
   * are equal to the bits of the integer argument.
   *
   * Consequently, zero and positive {@code int} values are mapped to a numerically equal
   * {@code long} value and negative {@code
   * int} values are mapped to a {@code long} value equal to the input plus 2<sup>32</sup>.
   *
   * @param x the value to convert to an unsigned {@code long}
   * @return the argument converted to {@code long} by an unsigned conversion
   * @since 1.8
   */
  // Duplicated from jdk8 Integer.toUnsignedLong
  public static long toUnsignedLong(int x) {
    return ((long) x) & 0xffffffffL;
  }
  /**
   * Sorts the data by the 16 bit prefix using Radix sort.
   * The resulting data will be partially sorted if you just
   * take into account the most significant 16 bits. The least
   * significant 16 bits are unsorted. Note that we treat int values
   * as unsigned integers (from 0 to 2^32).
   * @param data - the data (sorted in place)
   */
  public static void partialRadixSort(int[] data) {
    int[] low = new int[257];
    int[] high = new int[257];
    for (int value : data) {
      ++low[((value >>> 16) & 0xFF) + 1];
      ++high[(value >>> 24) + 1];
    }
    // avoid passes over the data if it's not required
    boolean sortLow = low[1] < data.length;
    boolean sortHigh = high[1] < data.length;
    if (!sortLow && !sortHigh) {
      return;
    }
    int[] copy = new int[data.length];
    if (sortLow) {
      for (int i = 1; i < low.length; ++i) {
        low[i] += low[i - 1];
      }
      for (int value : data) {
        copy[low[(value >>> 16) & 0xFF]++] = value;
      }
    }
    if (sortHigh) {
      for (int i = 1; i < high.length; ++i) {
        high[i] += high[i - 1];
      }
      if (sortLow) {
        for (int value : copy) {
          data[high[value >>> 24]++] = value;
        }
      } else {
        for (int value : data) {
          copy[high[value >>> 24]++] = value;
        }
        System.arraycopy(copy, 0, data, 0, data.length);
      }
    } else {
      System.arraycopy(copy, 0, data, 0, data.length);
    }
  }
  /**
   * Private constructor to prevent instantiation of utility class
   */
  private Util() {

  }
}
