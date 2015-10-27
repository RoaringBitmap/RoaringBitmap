/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import java.nio.Buffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import org.roaringbitmap.Util;

/**
 * Various useful methods for roaring bitmaps.
 * 
 * This class is similar to org.roaringbitmap.Util but meant to be used with
 * memory mapping.
 */
public final class BufferUtil {

    
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
        if (start == end)
            return;
        int firstword = start / 64;
        int endword = (end - 1) / 64;
        bitmap.put(firstword, bitmap.get(firstword) ^ ~(~0L << start));
        for (int i = firstword; i < endword; i++)
            bitmap.put(i, ~bitmap.get(i));
        bitmap.put(endword, bitmap.get(endword) ^ (~0L >>> -end));
    }

    /**
     * clear bits at start, start+1,..., end-1
     * 
     * @param bitmap array of words to be modified
     * @param start first index to be modified (inclusive)
     * @param end last index to be modified (exclusive)
     */
    public static void resetBitmapRange(LongBuffer bitmap, int start, int end) {
        if(isBackedBySimpleArray(bitmap)) {
            Util.resetBitmapRange(bitmap.array(), start, end);
            return;
        }
        if (start == end) return;
        int firstword = start / 64;
        int endword   = (end - 1 ) / 64;
        if(firstword == endword) {
            bitmap.put(firstword,bitmap.get(firstword) & ~((~0L << start) & (~0L >>> -end)));
          return;       
        }
        bitmap.put(firstword, bitmap.get(firstword) & (~(~0L << start)));
        for (int i = firstword+1; i < endword; i++)
            bitmap.put(i, 0L);
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
        if(isBackedBySimpleArray(bitmap)) {
            Util.setBitmapRange(bitmap.array(), start, end);
            return;
        }
        if (start == end) return;
        int firstword = start / 64;
        int endword   = (end - 1 ) / 64;
        if(firstword == endword) {
            bitmap.put(firstword,bitmap.get(firstword) | ((~0L << start) & (~0L >>> -end)));

          return;       
        }
        bitmap.put(firstword, bitmap.get(firstword) | (~0L << start));
        for (int i = firstword+1; i < endword; i++)
            bitmap.put(i, ~0L);
        bitmap.put(endword, bitmap.get(endword) | (~0L >>> -end));
    }

    /**
     * Find the smallest integer larger than pos such that array[pos]&gt;= min. If
     * none can be found, return length. Based on code by O. Kaser.
     * 
     * @param array container where we search
     * @param pos initial position
     * @param min minimal threshold
     * @param length how big should the array consider to be
     * @return x greater than pos such that array[pos] is at least as large as
     *         min, pos is is equal to length if it is not possible.
     */
    protected static int advanceUntil(ShortBuffer array, int pos, int length,
            short min) {
        int lower = pos + 1;

        // special handling for a possibly common sequential case
        if (lower >= length
                || toIntUnsigned(array.get(lower)) >= toIntUnsigned(min)) {
            return lower;
        }

        int spansize = 1; // could set larger
        // bootstrap an upper limit

        while (lower + spansize < length
                && toIntUnsigned(array.get(lower + spansize)) < toIntUnsigned(min))
            spansize *= 2; // hoping for compiler will reduce to
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
        lower += (spansize / 2);

        // else begin binary search
        // invariant: array[lower]<min && array[upper]>min
        while (lower + 1 != upper) {
            int mid = (lower + upper) / 2;
            short arraymid = array.get(mid);
            if (arraymid == min) {
                return mid;
            } else if (toIntUnsigned(arraymid) < toIntUnsigned(min))
                lower = mid;
            else
                upper = mid;
        }
        return upper;

    }

    protected static void fillArrayAND(short[] container, LongBuffer bitmap1,
            LongBuffer bitmap2) {
        int pos = 0;
        if (bitmap1.limit() != bitmap2.limit())
            throw new IllegalArgumentException("not supported");
        if (BufferUtil.isBackedBySimpleArray(bitmap1)
                && BufferUtil.isBackedBySimpleArray(bitmap2)) {
            int len = bitmap1.limit();
            long[] b1 = bitmap1.array();
            long[] b2 = bitmap2.array();
            for (int k = 0; k < len; ++k) {
                long bitset = b1[k] & b2[k];
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    container[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }
        } else {
            int len = bitmap1.limit();
            for (int k = 0; k < len; ++k) {
                long bitset = bitmap1.get(k) & bitmap2.get(k);
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    container[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }
        }
    }

    protected static void fillArrayANDNOT(short[] container,
            LongBuffer bitmap1, LongBuffer bitmap2) {
        int pos = 0;
        if (bitmap1.limit() != bitmap2.limit())
            throw new IllegalArgumentException("not supported");
        if (BufferUtil.isBackedBySimpleArray(bitmap1)
                && BufferUtil.isBackedBySimpleArray(bitmap2)) {
            int len = bitmap1.limit();
            long[] b1 = bitmap1.array();
            long[] b2 = bitmap2.array();
            for (int k = 0; k < len; ++k) {
                long bitset = b1[k] & (~b2[k]);
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    container[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }
        } else {
            int len = bitmap1.limit();
            for (int k = 0; k < len; ++k) {
                long bitset = bitmap1.get(k) & (~bitmap2.get(k));
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    container[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }
        }
    }

    protected static void fillArrayXOR(short[] container, LongBuffer bitmap1,
            LongBuffer bitmap2) {
        int pos = 0;
        if (bitmap1.limit() != bitmap2.limit())
            throw new IllegalArgumentException("not supported");
        if (BufferUtil.isBackedBySimpleArray(bitmap1)
                && BufferUtil.isBackedBySimpleArray(bitmap2)) {
            int len = bitmap1.limit();
            long[] b1 = bitmap1.array();
            long[] b2 = bitmap2.array();
            for (int k = 0; k < len; ++k) {
                long bitset = b1[k] ^ b2[k];
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    container[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }

        } else {
            int len = bitmap1.limit();
            for (int k = 0; k < len; ++k) {
                long bitset = bitmap1.get(k) ^ bitmap2.get(k);
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    container[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }
        }
    }

    /**
     * From the cardinality of a container, compute the corresponding size in
     * bytes of the container.  Additional information is required
     * if the container is run encoded.
     * 
     * @param card
     *            the cardinality if this is not run encoded, otherwise ignored
     * @param numRuns
     *            number of runs if run encoded, othewise ignored
     * @param isRunEncoded
     *            boolean 
     *
     * @return the size in bytes
     */


    // this is ugly now.
    
    protected static int getSizeInBytesFromCardinalityEtc(int card, int numRuns, boolean isRunEncoded) {
        if (isRunEncoded)
            return 2 + numRuns * 2 * 2;  // each run uses 2 shorts, plus the initial short giving num runs
        boolean isBitmap = card > MappeableArrayContainer.DEFAULT_MAX_SIZE;
        if (isBitmap)
            return MappeableBitmapContainer.MAX_CAPACITY / 8;
        else
            return card * 2;

    }


    protected static boolean isBackedBySimpleArray(Buffer b) {
        return b.hasArray() && (b.arrayOffset() == 0);
    }

    protected static short highbits(int x) {
        return (short) (x >>> 16);
    }

    protected static short lowbits(int x) {
        return (short) (x & 0xFFFF);
    }

    protected static short maxLowBit() {
        return (short) 0xFFFF;
    }

    protected static int maxLowBitAsInteger() {
        return 0xFFFF;
    }

    protected static void arraycopy(ShortBuffer src, int srcPos, ShortBuffer dest, int destPos, int length) {
      if(BufferUtil.isBackedBySimpleArray(src) && BufferUtil.isBackedBySimpleArray(dest)) {
          System.arraycopy(src.array(), srcPos,  dest.array(), destPos, length);
      } else {
          if(srcPos < destPos) {
              for(int k = length - 1; k >= 0 ; --k) {
                  dest.put(destPos + k , src.get(k + srcPos));  
              }
          } else {
              for(int k = 0; k < length ; ++k) {
                  dest.put(destPos + k , src.get(k + srcPos));  
              }              
          }
      }
    }

    protected static int toIntUnsigned(short x) {
        return x & 0xFFFF;
    }
    
    /**
     * Look for value k in buffer in the range [begin,end). If the value
     * is found, return its index. If not, return -(i+1) where i is the
     * index where the value would be inserted. 
     * The buffer is assumed to contain sorted values where shorts are
     * interpreted as unsigned integers.
     * 
     * @param array buffer where we search
     * @param begin first index (inclusive)
     * @param end last index (exclusive)
     * @param k value we search for
     * @return count
     */
    public static int unsignedBinarySearch(final ShortBuffer array, final int begin,
            final int end,  final short k) {
        if(BufferUtil.USE_HYBRID_BINSEARCH)
          return hybridUnsignedBinarySearch(array,begin,end, k);
        else 
          return branchyUnsignedBinarySearch(array,begin,end, k);
    }
    
    // optimization flag: whether to use hybrid binary search: hybrid formats
    // combine a binary search with a sequential search 
    protected static boolean USE_HYBRID_BINSEARCH = false;

    
    // starts with binary search and finishes with a sequential search
    protected static int hybridUnsignedBinarySearch(final ShortBuffer array, final int begin,
            final int end, final short k) {
        final int ikey = toIntUnsigned(k);
        // next line accelerates the possibly common case where the value would be inserted at the end
        if((end>0) && (toIntUnsigned(array.get(end-1)) < ikey)) return - end - 1;
        int low = begin;
        int high = end - 1;
        // 32 in the next line matches the size of a cache line
        while (low + 32 <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = toIntUnsigned(array.get(middleIndex));

            if (middleValue < ikey)
                low = middleIndex + 1;
            else if (middleValue > ikey)
                high = middleIndex - 1;
            else
                return middleIndex;
        }
        // we finish the job with a sequential search 
        int x = low;
        for(; x <= high; ++x) {
            final int val = toIntUnsigned(array.get(x));
            if(val >= ikey) {
                if(val == ikey) return x;
                break;
            }
        }
        return -(x + 1);
    }

    
    protected static int branchyUnsignedBinarySearch(final ShortBuffer array, final int begin,
            final int end, final short k) {
        final int ikey = toIntUnsigned(k);
        // next line accelerates the possibly common case where the value would be inserted at the end
        if((end>0) && (toIntUnsigned(array.get(end-1)) < ikey)) return - end - 1;
        int low = begin;
        int high = end - 1;
        while (low <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = toIntUnsigned(array.get(middleIndex));

            if (middleValue < ikey)
                low = middleIndex + 1;
            else if (middleValue > ikey)
                high = middleIndex - 1;
            else
                return middleIndex;
        }
        return -(low + 1);
    }
    

    protected static int unsignedDifference(final ShortBuffer set1,
            final int length1, final ShortBuffer set2, final int length2,
            final short[] buffer) {
        int pos = 0;
        int k1 = 0, k2 = 0;
        if (0 == length2) {
            set1.get(buffer, 0, length1 );
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

    protected static int unsignedExclusiveUnion2by2(final ShortBuffer set1,
            final int length1, final ShortBuffer set2, final int length2,
            final short[] buffer) {
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
        //return pos;
    }

    protected static int unsignedIntersect2by2(final ShortBuffer set1,
            final int length1, final ShortBuffer set2, final int length2,
            final short[] buffer) {
        if (length1 * 64 < length2) {
            return unsignedOneSidedGallopingIntersect2by2(set1, length1, set2,
                    length2, buffer);
        } else if (length2 * 64 < length1) {
            return unsignedOneSidedGallopingIntersect2by2(set2, length2, set1,
                    length1, buffer);
        } else {
            return unsignedLocalIntersect2by2(set1, length1, set2, length2,
                    buffer);
        }
    }

    protected static int unsignedOneSidedGallopingIntersect2by2(
            final ShortBuffer smallSet, final int smallLength,
            final ShortBuffer largeSet, final int largeLength,
            final short[] buffer) {
        if (0 == smallLength)
            return 0;
        int k1 = 0;
        int k2 = 0;
        int pos = 0;

        short s1 = largeSet.get(k1);
        short s2 = smallSet.get(k2);
        while (true) {
            if (toIntUnsigned(s1) < toIntUnsigned(s2)) {
                k1 = advanceUntil(largeSet, k1, largeLength, s2);
                if (k1 == largeLength)
                    break;
                s1 = largeSet.get(k1);
            }
            if (toIntUnsigned(s2) < toIntUnsigned(s1)) {
                ++k2;
                if (k2 == smallLength)
                    break;
                s2 = smallSet.get(k2);
            } else {
                // (set2.get(k2) == set1.get(k1))
                buffer[pos++] = s2;
                ++k2;
                if (k2 == smallLength)
                    break;
                s2 = smallSet.get(k2);
                k1 = advanceUntil(largeSet, k1, largeLength, s2);
                if (k1 == largeLength)
                    break;
                s1 = largeSet.get(k1);
            }

        }
        return pos;

    }


    protected static int unsignedLocalIntersect2by2(final ShortBuffer set1,
            final int length1, final ShortBuffer set2, final int length2,
            final short[] buffer) {
        if ((0 == length1) || (0 == length2))
            return 0;
        int k1 = 0;
        int k2 = 0;
        int pos = 0;
        short s1 = set1.get(k1);
        short s2 = set2.get(k2);

        mainwhile: while (true) {
            if (toIntUnsigned(s2) < toIntUnsigned(s1)) {
                do {
                    ++k2;
                    if (k2 == length2)
                        break mainwhile;
                    s2 = set2.get(k2);

                } while (toIntUnsigned(s2) < toIntUnsigned(s1));
            }
            if (toIntUnsigned(s1) < toIntUnsigned(s2)) {
                do {
                    ++k1;
                    if (k1 == length1)
                        break mainwhile;
                    s1 = set1.get(k1);

                } while (toIntUnsigned(s1) < toIntUnsigned(s2));
            } else {
                // (set2.get(k2) == set1.get(k1))
                buffer[pos++] = s1;
                ++k1;
                if (k1 == length1)
                    break;
                s1 = set1.get(k1);
                ++k2;
                if (k2 == length2)
                    break;
                s2 = set2.get(k2);

            }
        }
        return pos;
    }

    protected static int oldunsignedOneSidedGallopingIntersect2by2(
            final ShortBuffer smallSet, final int smallLength,
            final ShortBuffer largeSet, final int largeLength,
            final short[] buffer) {
        if (0 == smallLength)
            return 0;
        int k1 = 0;
        int k2 = 0;
        int pos = 0;
        while (true) {
            if (toIntUnsigned(largeSet.get(k1)) < toIntUnsigned(smallSet
                    .get(k2))) {
                k1 = advanceUntil(largeSet, k1, largeLength, smallSet.get(k2));
                if (k1 == largeLength)
                    break;
            }
            if (toIntUnsigned(smallSet.get(k2)) < toIntUnsigned(largeSet
                    .get(k1))) {
                ++k2;
                if (k2 == smallLength)
                    break;
            } else {
                // (set2.get(k2) == set1.get(k1))
                buffer[pos++] = smallSet.get(k2);
                ++k2;
                if (k2 == smallLength)
                    break;
                k1 = advanceUntil(largeSet, k1, largeLength, smallSet.get(k2));
                if (k1 == largeLength)
                    break;
            }

        }
        return pos;

    }
    
    protected static int unsignedUnion2by2(final ShortBuffer set1,
            final int length1, final ShortBuffer set2, final int length2,
            final short[] buffer) {
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
            int v1 = toIntUnsigned(s1);
            int v2 = toIntUnsigned(s2);
            if (v1 < v2) {
                buffer[pos++] = s1;
                ++k1;
                if (k1 >= length1) {
                    set2.position(k2);
                    set2.get(buffer, pos, length2 - k2);
                    return pos + length2 - k2;
                }
                s1 = set1.get(k1);
            } else if (v1 == v2) {
                buffer[pos++] = s1;
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
            } else {// if (set1.get(k1)>set2.get(k2))
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
        //return pos;
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

    /**
     * Private constructor to prevent instantiation of utility class
     */
    private BufferUtil() {
    }
    
    /**
     * Checks if two arrays intersect
     *
     * @param set1    first array
     * @param length1 length of first array
     * @param set2    second array
     * @param length2 length of second array
     * @return true if they intersect
     */
    public static boolean unsignedIntersects(ShortBuffer set1,
            int length1, ShortBuffer set2, int length2) {
        if ((0 == length1) || (0 == length2))
            return false;
        int k1 = 0;
        int k2 = 0;
        
        // could be more efficient with galloping
        short s1 = set1.get(k1);
        short s2 = set2.get(k2);

        mainwhile: while (true) {
            if (toIntUnsigned(s2) < toIntUnsigned(s1)) {
                do {
                    ++k2;
                    if (k2 == length2)
                        break mainwhile;
                    s2 = set2.get(k2);
                } while (toIntUnsigned(s2) < toIntUnsigned(s1));
            }
            if (toIntUnsigned(s1) < toIntUnsigned(s2)) {
                do {
                    ++k1;
                    if (k1 == length1)
                        break mainwhile;
                    s1 = set1.get(k1);
                } while (toIntUnsigned(s1) < toIntUnsigned(s2));
            } else {
                return true;
            }
        }
        return false;
    }
}
