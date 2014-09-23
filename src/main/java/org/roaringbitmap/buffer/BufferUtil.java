/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;

import java.nio.LongBuffer;
import java.nio.ShortBuffer;

/**
 * Various useful methods for roaring bitmaps.
 * 
 * This class is similar to org.roaringbitmap.Util but meant to be used with
 * memory mapping.
 */
public final class BufferUtil {

    /**
     * Find the smallest integer larger than pos such that array[pos]>= min. If
     * none can be found, return length. Based on code by O. Kaser.
     * 
     * @param array
     * @param pos
     * @param min
     * @return x greater than pos such that array[pos] is at least as large as
     *         min, pos is is equal to length if it is not possible.
     */
    private static int advanceUntil(ShortBuffer array, int pos, int length,
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
            if (array.get(mid) == min) {
                return mid;
            } else if (toIntUnsigned(array.get(mid)) < toIntUnsigned(min))
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
        for (int k = 0; k < bitmap1.limit(); ++k) {
            long bitset = bitmap1.get(k) & bitmap2.get(k);
            while (bitset != 0) {
                final long t = bitset & -bitset;
                container[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                bitset ^= t;
            }
        }
    }

    protected static void fillArrayANDNOT(short[] container,
            LongBuffer bitmap1, LongBuffer bitmap2) {
        int pos = 0;
        if (bitmap1.limit() != bitmap2.limit())
            throw new IllegalArgumentException("not supported");
        for (int k = 0; k < bitmap1.limit(); ++k) {
            long bitset = bitmap1.get(k) & (~bitmap2.get(k));
            while (bitset != 0) {
                final long t = bitset & -bitset;
                container[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                bitset ^= t;
            }
        }
    }

    protected static void fillArrayXOR(short[] container, LongBuffer bitmap1,
            LongBuffer bitmap2) {
        int pos = 0;
        if (bitmap1.limit() != bitmap2.limit())
            throw new IllegalArgumentException("not supported");
        for (int k = 0; k < bitmap1.limit(); ++k) {
            long bitset = bitmap1.get(k) ^ bitmap2.get(k);
            while (bitset != 0) {
                final long t = bitset & -bitset;
                container[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                bitset ^= t;
            }
        }
    }

    /**
     * From the cardinality of a container, compute the corresponding size in
     * bytes of the container.
     * 
     * @param card
     *            the cardinality
     * @return the size in bytes
     */
    public static int getSizeInBytesFromCardinality(int card) {
        boolean isBitmap = card > MappeableArrayContainer.DEFAULT_MAX_SIZE;
        if (isBitmap)
            return MappeableBitmapContainer.MAX_CAPACITY / 8;
        else
            return card * 2;

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

    protected static int toIntUnsigned(short x) {
        return x & 0xFFFF;
    }

    protected static int unsignedBinarySearch(ShortBuffer array, int begin,
            int end, short k) {
        int low = begin;
        int high = end - 1;
        final int ikey = toIntUnsigned(k);

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
            for (int k = 0; k < length1; ++k)
                buffer[k] = set1.get(k);
            return length1;
        }
        if (0 == length1) {
            return 0;
        }
        while (true) {
            if (toIntUnsigned(set1.get(k1)) < toIntUnsigned(set2.get(k2))) {
                buffer[pos++] = set1.get(k1);
                ++k1;
                if (k1 >= length1) {
                    break;
                }
            } else if (toIntUnsigned(set1.get(k1)) == toIntUnsigned(set2
                    .get(k2))) {
                ++k1;
                ++k2;
                if (k1 >= length1) {
                    break;
                }
                if (k2 >= length2) {
                    for (; k1 < length1; ++k1)
                        buffer[pos++] = set1.get(k1);
                    break;
                }
            } else {// if (val1>val2)
                ++k2;
                if (k2 >= length2) {
                    for (; k1 < length1; ++k1)
                        buffer[pos++] = set1.get(k1);
                    break;
                }
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
            for (int k = 0; k < length1; ++k)
                buffer[k] = set1.get(k);
            return length1;
        }
        if (0 == length1) {
            for (int k = 0; k < length2; ++k)
                buffer[k] = set2.get(k);
            return length2;
        }
        while (true) {
            if (toIntUnsigned(set1.get(k1)) < toIntUnsigned(set2.get(k2))) {
                buffer[pos++] = set1.get(k1);
                ++k1;
                if (k1 >= length1) {
                    for (; k2 < length2; ++k2)
                        buffer[pos++] = set2.get(k2);
                    break;
                }
            } else if (toIntUnsigned(set1.get(k1)) == toIntUnsigned(set2
                    .get(k2))) {
                ++k1;
                ++k2;
                if (k1 >= length1) {
                    for (; k2 < length2; ++k2)
                        buffer[pos++] = set2.get(k2);
                    break;
                }
                if (k2 >= length2) {
                    for (; k1 < length1; ++k1)
                        buffer[pos++] = set1.get(k1);
                    break;
                }
            } else {// if (val1>val2)
                buffer[pos++] = set2.get(k2);
                ++k2;
                if (k2 >= length2) {
                    for (; k1 < length1; ++k1)
                        buffer[pos++] = set1.get(k1);
                    break;
                }
            }
        }
        return pos;
    }

    protected static int unsignedIntersect2by2(final ShortBuffer set1,
            final int length1, final ShortBuffer set2, final int length2,
            final short[] buffer) {
        if (set1.limit() * 64 < set2.limit()) {
            return unsignedOneSidedGallopingIntersect2by2(set1, length1, set2,
                    length2, buffer);
        } else if (set2.limit() * 64 < set1.limit()) {
            return unsignedOneSidedGallopingIntersect2by2(set2, length2, set1,
                    length1, buffer);
        } else {
            return unsignedLocalIntersect2by2(set1, length1, set2, length2,
                    buffer);
        }
    }

    protected static int unsignedLocalIntersect2by2(final ShortBuffer set1,
            final int length1, final ShortBuffer set2, final int length2,
            final short[] buffer) {
        if ((0 == length1) || (0 == length2))
            return 0;
        int k1 = 0;
        int k2 = 0;
        int pos = 0;

        mainwhile: while (true) {
            if (toIntUnsigned(set2.get(k2)) < toIntUnsigned(set1.get(k1))) {
                do {
                    ++k2;
                    if (k2 == length2)
                        break mainwhile;
                } while (toIntUnsigned(set2.get(k2)) < toIntUnsigned(set1
                        .get(k1)));
            }
            if (toIntUnsigned(set1.get(k1)) < toIntUnsigned(set2.get(k2))) {
                do {
                    ++k1;
                    if (k1 == length1)
                        break mainwhile;
                } while (toIntUnsigned(set1.get(k1)) < toIntUnsigned(set2
                        .get(k2)));
            } else {
                // (set2.get(k2) == set1.get(k1))
                buffer[pos++] = set1.get(k1);
                ++k1;
                if (k1 == length1)
                    break;
                ++k2;
                if (k2 == length2)
                    break;
            }
        }
        return pos;
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
            for (int k = 0; k < length1; ++k)
                buffer[k] = set1.get(k);
            return length1;
        }
        if (0 == length1) {
            for (int k = 0; k < length2; ++k)
                buffer[k] = set2.get(k);
            return length2;
        }
        while (true) {
            if (toIntUnsigned(set1.get(k1)) < toIntUnsigned(set2.get(k2))) {
                buffer[pos++] = set1.get(k1);
                ++k1;
                if (k1 >= length1) {
                    for (; k2 < length2; ++k2)
                        buffer[pos++] = set2.get(k2);
                    break;
                }
            } else if (toIntUnsigned(set1.get(k1)) == toIntUnsigned(set2
                    .get(k2))) {
                buffer[pos++] = set1.get(k1);
                ++k1;
                ++k2;
                if (k1 >= length1) {
                    for (; k2 < length2; ++k2)
                        buffer[pos++] = set2.get(k2);
                    break;
                }
                if (k2 >= length2) {
                    for (; k1 < length1; ++k1)
                        buffer[pos++] = set1.get(k1);
                    break;
                }
            } else {// if (set1.get(k1)>set2.get(k2))
                buffer[pos++] = set2.get(k2);
                ++k2;
                if (k2 >= length2) {
                    for (; k1 < length1; ++k1)
                        buffer[pos++] = set1.get(k1);
                    break;
                }
            }
        }
        return pos;
    }

    /**
     * Private constructor to prevent instantiation of utility class
     */
    private BufferUtil() {
    }
}
