/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.ShortIterator;
import org.roaringbitmap.Util;

import java.io.*;
import java.nio.LongBuffer;
import java.util.Iterator;

/**
 * Simple bitset-like container. Unlike org.roaringbitmap.BitmapContainer, this
 * class uses a LongBuffer to store data.
 */
public final class MappeableBitmapContainer extends MappeableContainer
        implements Cloneable, Serializable {
    protected static final int MAX_CAPACITY = 1 << 16;

    private static final long serialVersionUID = 2L;

    private static boolean USE_IN_PLACE = true; // optimization flag

    LongBuffer bitmap;

    int cardinality;

    /**
     * Create a bitmap container with all bits set to false
     */
    public MappeableBitmapContainer() {
        this.cardinality = 0;
        this.bitmap = LongBuffer.allocate(MAX_CAPACITY / 64);
    }

    /**
     * Create a bitmap container with a run of ones from firstOfRun to
     * lastOfRun, inclusive caller must ensure that the range isn't so small
     * that an ArrayContainer should have been created instead
     * 
     * @param firstOfRun
     *            first index
     * @param lastOfRun
     *            last index (range is exclusive)
     */
    public MappeableBitmapContainer(final int firstOfRun, final int lastOfRun) {
        // TODO: this can be optimized for performance
        this.cardinality = lastOfRun - firstOfRun ;
        this.bitmap = LongBuffer.allocate(MAX_CAPACITY / 64);
        if (this.cardinality == MAX_CAPACITY) {// perhaps a common case
            for (int k = 0; k < bitmap.limit(); ++k)
                bitmap.put(k, -1L);
        } else {
            final int firstWord = firstOfRun / 64;
            final int lastWord = (lastOfRun - 1) / 64;
            final int zeroPrefixLength = firstOfRun & 63;
            final int zeroSuffixLength = 63 - ((lastOfRun - 1) & 63);
            for (int k = firstWord; k < lastWord + 1; ++k)
                bitmap.put(k, -1L);
            bitmap.put(firstWord, bitmap.get(firstWord)
                    ^ ((1L << zeroPrefixLength) - 1));
            final long blockOfOnes = (1L << zeroSuffixLength) - 1;
            final long maskOnLeft = blockOfOnes << (64 - zeroSuffixLength);
            bitmap.put(lastWord, bitmap.get(lastWord) ^ maskOnLeft);
        }
    }

    MappeableBitmapContainer(int newCardinality, LongBuffer newBitmap) {
        this.cardinality = newCardinality;
        this.bitmap = LongBuffer.allocate(newBitmap.limit());
        newBitmap.rewind();
        this.bitmap.put(newBitmap);
    }

    /**
     * Construct a new BitmapContainer backed by the provided LongBuffer.
     * 
     * @param array
     *            LongBuffer where the data is stored
     * @param initCardinality
     *            cardinality (number of values stored)
     */
    public MappeableBitmapContainer(final LongBuffer array,
            final int initCardinality) {
        if (array.limit() != MAX_CAPACITY / 64)
            throw new RuntimeException(
                    "Mismatch between buffer and storage requirements: "
                            + array.limit() + " vs. " + MAX_CAPACITY / 64);
        this.cardinality = initCardinality;
        this.bitmap = array;
    }

    @Override
    public MappeableContainer add(final short i) {
        final int x = BufferUtil.toIntUnsigned(i);
        final long previous = bitmap.get(x / 64);
        bitmap.put(x / 64, previous | (1l << x));
        cardinality += (previous ^ bitmap.get(x / 64)) >>> x;
        return this;
    }

    @Override
    public MappeableArrayContainer and(final MappeableArrayContainer value2) {

        final MappeableArrayContainer answer = new MappeableArrayContainer(
                value2.content.limit());
        short[] sarray = answer.content.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content)) {
            short[] c = value2.content.array();
            for (int k = 0; k < value2.getCardinality(); ++k)
                if (this.contains(c[k]))
                    sarray[answer.cardinality++] = c[k];

        } else
            for (int k = 0; k < value2.getCardinality(); ++k)
                if (this.contains(value2.content.get(k)))
                    sarray[answer.cardinality++] = value2.content.get(k);
        return answer;
    }

    @Override
    public MappeableContainer and(final MappeableBitmapContainer value2) {
        int newCardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
            long[] tb = this.bitmap.array();
            long[] v2b = value2.bitmap.array();
            for (int k = 0; k < this.bitmap.limit(); ++k) {
                newCardinality += Long.bitCount(tb[k] & v2b[k]);
            }
        } else
            for (int k = 0; k < this.bitmap.limit(); ++k) {
                newCardinality += Long.bitCount(this.bitmap.get(k)
                        & value2.bitmap.get(k));
            }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            final MappeableBitmapContainer answer = new MappeableBitmapContainer();
            long[] bitArray = answer.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
                long[] tb = this.bitmap.array();
                long[] v2b = value2.bitmap.array();
                for (int k = 0; k < answer.bitmap.limit(); ++k) {
                    bitArray[k] = tb[k] & v2b[k];
                }
            } else
                for (int k = 0; k < answer.bitmap.limit(); ++k) {
                    bitArray[k] = this.bitmap.get(k) & value2.bitmap.get(k);
                }
            answer.cardinality = newCardinality;
            return answer;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap))
            org.roaringbitmap.Util.fillArrayAND(ac.content.array(),
                    this.bitmap.array(), value2.bitmap.array());
        else
            BufferUtil.fillArrayAND(ac.content.array(), this.bitmap,
                    value2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    @Override
    public MappeableContainer andNot(final MappeableArrayContainer value2) {
        final MappeableBitmapContainer answer = clone();
        long[] bitArray = answer.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content) && BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            short[] v2 = value2.content.array();
            long[] ba = this.bitmap.array();
            for (int k = 0; k < value2.cardinality; ++k) {
                final int i = BufferUtil.toIntUnsigned(v2[k]) >>> 6;
                bitArray[i] &= (~(1l << v2[k]));
                answer.cardinality -= (bitArray[i] ^ ba[i]) >>> v2[k];
            }
        } else
            for (int k = 0; k < value2.cardinality; ++k) {
                final int i = BufferUtil.toIntUnsigned(value2.content.get(k)) >>> 6;
                bitArray[i] &= (~(1l << value2.content.get(k)));
                answer.cardinality -= (bitArray[i] ^ this.bitmap.get(i)) >>> value2.content
                        .get(k);
            }
        if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public MappeableContainer andNot(final MappeableBitmapContainer value2) {

        int newCardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
            long[] b = this.bitmap.array();
            long[] v2 = value2.bitmap.array();
            for (int k = 0; k < this.bitmap.limit(); ++k) {
                newCardinality += Long.bitCount(b[k] & (~v2[k]));
            }

        } else
            for (int k = 0; k < this.bitmap.limit(); ++k) {
                newCardinality += Long.bitCount(this.bitmap.get(k)
                        & (~value2.bitmap.get(k)));
            }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            final MappeableBitmapContainer answer = new MappeableBitmapContainer();
            long[] bitArray = answer.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
                long[] b = this.bitmap.array();
                long[] v2 = value2.bitmap.array();

                for (int k = 0; k < answer.bitmap.limit(); ++k) {
                    bitArray[k] = b[k] & (~v2[k]);
                }
            } else
                for (int k = 0; k < answer.bitmap.limit(); ++k) {
                    bitArray[k] = this.bitmap.get(k) & (~value2.bitmap.get(k));
                }
            answer.cardinality = newCardinality;
            return answer;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap))
            org.roaringbitmap.Util.fillArrayANDNOT(ac.content.array(),
                    this.bitmap.array(), value2.bitmap.array());
        else
            BufferUtil.fillArrayANDNOT(ac.content.array(), this.bitmap,
                    value2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    @Override
    public void clear() {
        if (cardinality != 0) {
            cardinality = 0;
            for (int k = 0; k < bitmap.limit(); ++k)
                bitmap.put(k, 0);
        }
    }

    @Override
    public MappeableBitmapContainer clone() {
        return new MappeableBitmapContainer(this.cardinality, this.bitmap);
    }

    @Override
    public boolean contains(final short i) {
        final int x = BufferUtil.toIntUnsigned(i);
        return (bitmap.get(x / 64) & (1l << x)) != 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MappeableBitmapContainer) {
            final MappeableBitmapContainer srb = (MappeableBitmapContainer) o;
            if (srb.cardinality != this.cardinality)
                return false;
            if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(srb.bitmap)) {
                long[] b = this.bitmap.array();
                long[] s = srb.bitmap.array();
                for (int k = 0; k < this.bitmap.limit(); ++k)
                    if (b[k] != s[k])
                        return false;
            } else
                for (int k = 0; k < this.bitmap.limit(); ++k)
                    if (this.bitmap.get(k) != srb.bitmap.get(k))
                        return false;
            return true;

        }
        return false;
    }

    /**
     * Fill the array with set bits
     * 
     * @param array
     *            container (should be sufficiently large)
     */
    protected void fillArray(final short[] array) {
        int pos = 0;
        if (BufferUtil.isBackedBySimpleArray(bitmap)) {
            long[] b = bitmap.array();
            for (int k = 0; k < bitmap.limit(); ++k) {
                long bitset = b[k];
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    array[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }

        } else
            for (int k = 0; k < bitmap.limit(); ++k) {
                long bitset = bitmap.get(k);
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    array[pos++] = (short) (k * 64 + Long.bitCount(t - 1));
                    bitset ^= t;
                }
            }
    }

    @Override
    public void fillLeastSignificant16bits(int[] x, int i, int mask) {
        int pos = i;
        if (BufferUtil.isBackedBySimpleArray(bitmap)) {
            long[] b = bitmap.array();
            for (int k = 0; k < bitmap.limit(); ++k) {
                long bitset = b[k];
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    x[pos++] = (k * 64 + Long.bitCount(t - 1)) | mask;
                    bitset ^= t;
                }
            }

        } else
            for (int k = 0; k < bitmap.limit(); ++k) {
                long bitset = bitmap.get(k);
                while (bitset != 0) {
                    final long t = bitset & -bitset;
                    x[pos++] = (k * 64 + Long.bitCount(t - 1)) | mask;
                    bitset ^= t;
                }
            }
    }

    @Override
    protected int getArraySizeInBytes() {
        return MAX_CAPACITY / 8;
    }

    @Override
    public int getCardinality() {
        return cardinality;
    }

    @Override
    public ShortIterator getShortIterator() {
        return new MappeableBitmapContainerShortIterator(this);
    }

    @Override
    public ShortIterator getReverseShortIterator() {
        return new ReverseMappeableBitmapContainerShortIterator(this);
    }

    @Override
    public int getSizeInBytes() {
        return this.bitmap.limit() * 8;
    }

    @Override
    public int hashCode() {
        long hash = 0;
        for (int k = 0; k < this.bitmap.limit(); ++k)
            hash += 31 * hash + this.bitmap.get(k);
        return (int) hash;
    }

    @Override
    public MappeableContainer iand(final MappeableArrayContainer b2) {
        return b2.and(this);// no inplace possible
    }

    @Override
    public MappeableContainer iand(final MappeableBitmapContainer b2) {
        int newCardinality = 0;
        for (int k = 0; k < this.bitmap.limit(); ++k) {
            newCardinality += Long.bitCount(this.bitmap.get(k)
                    & b2.bitmap.get(k));
        }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            for (int k = 0; k < this.bitmap.limit(); ++k) {
                this.bitmap.put(k, this.bitmap.get(k) & b2.bitmap.get(k));
            }
            this.cardinality = newCardinality;
            return this;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);
        BufferUtil.fillArrayAND(ac.content.array(), this.bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    @Override
    public MappeableContainer iandNot(final MappeableArrayContainer b2) {
        for (int k = 0; k < b2.cardinality; ++k) {
            this.remove(b2.content.get(k));
        }
        if (cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return this.toArrayContainer();
        return this;
    }

    @Override
    public MappeableContainer iandNot(final MappeableBitmapContainer b2) {
        int newCardinality = 0;

        long[] b = this.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
            long[] b2Arr = b2.bitmap.array();
            for (int k = 0; k < this.bitmap.limit(); ++k) {
                newCardinality += Long.bitCount(b[k] & (~b2Arr[k]));
            }
            if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
                for (int k = 0; k < this.bitmap.limit(); ++k) {
                    this.bitmap.put(k, b[k] & (~b2Arr[k]));
                }
                this.cardinality = newCardinality;
                return this;
            }
            final MappeableArrayContainer ac = new MappeableArrayContainer(
                    newCardinality);
            org.roaringbitmap.Util
                    .fillArrayANDNOT(ac.content.array(), b, b2Arr);
            ac.cardinality = newCardinality;
            return ac;

        }

        for (int k = 0; k < this.bitmap.limit(); ++k) {
            newCardinality += Long.bitCount(b[k] & (~b2.bitmap.get(k)));
        }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            for (int k = 0; k < this.bitmap.limit(); ++k) {
                b[k] &= (~b2.bitmap.get(k));
            }
            this.cardinality = newCardinality;
            return this;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);

        BufferUtil.fillArrayANDNOT(ac.content.array(), this.bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }


    @Override
    public MappeableContainer inot(final int firstOfRange, final int lastOfRange) {
        return not(this, firstOfRange, lastOfRange); 
    }

    @Override
    public MappeableBitmapContainer ior(final MappeableArrayContainer value2) {
        long[] b = this.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content)) {

            short[] v2 = value2.content.array();
            for (int k = 0; k < value2.cardinality; ++k) {
                final int i = BufferUtil.toIntUnsigned(v2[k]) >>> 6;
                this.cardinality += ((~b[i]) & (1l << value2.content.get(k))) >>> v2[k];
                b[i] |= (1l << v2[k]);
            }
            return this;
        }

        for (int k = 0; k < value2.cardinality; ++k) {
            final int i = BufferUtil.toIntUnsigned(value2.content.get(k)) >>> 6;
            this.cardinality += ((~b[i]) & (1l << value2.content.get(k))) >>> value2.content
                    .get(k);
            b[i] |= (1l << value2.content.get(k));
        }
        return this;
    }

    @Override
    public MappeableContainer ior(final MappeableBitmapContainer b2) {
        long[] b = this.bitmap.array();
        this.cardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
            long[] b2Arr = b2.bitmap.array();
            for (int k = 0; k < this.bitmap.limit(); k++) {
                b[k] |= b2Arr[k];
                this.cardinality += Long.bitCount(b[k]);
            }
            return this;
        }
        for (int k = 0; k < this.bitmap.limit(); k++) {
            b[k] |= b2.bitmap.get(k);
            this.cardinality += Long.bitCount(b[k]);
        }
        return this;
    }

    @Override
    public Iterator<Short> iterator() {
        return new Iterator<Short>() {
            final ShortIterator si = MappeableBitmapContainer.this.getShortIterator();

            @Override
            public boolean hasNext() {
                return si.hasNext();
            }

            @Override
            public Short next() {
                return si.next();
            }

            @Override
            public void remove() {
                //TODO: implement
                throw new RuntimeException("unsupported operation: remove");
            }
        };
    }

    @Override
    public MappeableContainer ixor(final MappeableArrayContainer value2) {
        long[] b = bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content)) {
            short[] v2 = value2.content.array();
            for (int k = 0; k < value2.getCardinality(); ++k) {
                final int index = BufferUtil.toIntUnsigned(v2[k]) >>> 6;
                this.cardinality += 1 - 2 * ((b[index] & (1l << v2[k])) >>> v2[k]);
                b[index] ^= (1l << v2[k]);
            }

        } else
            for (int k = 0; k < value2.getCardinality(); ++k) {
                final int index = BufferUtil.toIntUnsigned(value2.content
                        .get(k)) >>> 6;
                this.cardinality += 1 - 2 * ((b[index] & (1l << value2.content
                        .get(k))) >>> value2.content.get(k));
                b[index] ^= (1l << value2.content.get(k));
            }
        if (this.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            return this.toArrayContainer();
        }
        return this;
    }

    @Override
    public MappeableContainer ixor(MappeableBitmapContainer b2) {
        long[] b = bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(b2.bitmap)) {
            long[] b2Arr = b2.bitmap.array();

            int newCardinality = 0;
            for (int k = 0; k < this.bitmap.limit(); ++k) {
                newCardinality += Long.bitCount(b[k] ^ b2Arr[k]);
            }
            if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
                for (int k = 0; k < this.bitmap.limit(); ++k) {
                    b[k] ^= b2Arr[k];
                }
                this.cardinality = newCardinality;
                return this;
            }
            final MappeableArrayContainer ac = new MappeableArrayContainer(
                    newCardinality);

            org.roaringbitmap.Util.fillArrayXOR(ac.content.array(), b, b2Arr);
            ac.cardinality = newCardinality;
            return ac;

        }
        int newCardinality = 0;
        for (int k = 0; k < this.bitmap.limit(); ++k) {
            newCardinality += Long.bitCount(b[k] ^ b2.bitmap.get(k));
        }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            for (int k = 0; k < this.bitmap.limit(); ++k) {
                b[k] ^= b2.bitmap.get(k);
            }
            this.cardinality = newCardinality;
            return this;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);

        BufferUtil.fillArrayXOR(ac.content.array(), this.bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    protected void loadData(final MappeableArrayContainer arrayContainer) {
        this.cardinality = arrayContainer.cardinality;
        long[] bitArray = bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(bitmap) && BufferUtil.isBackedBySimpleArray(arrayContainer.content)) {
            long[] b = bitmap.array();
            short[] ac = arrayContainer.content.array();
            for (int k = 0; k < arrayContainer.cardinality; ++k) {
                final short x = ac[k];
                bitArray[BufferUtil.toIntUnsigned(x) / 64] = b[BufferUtil
                        .toIntUnsigned(x) / 64] | (1l << x);
            }

        } else
            for (int k = 0; k < arrayContainer.cardinality; ++k) {
                final short x = arrayContainer.content.get(k);
                bitArray[BufferUtil.toIntUnsigned(x) / 64] = bitmap
                        .get(BufferUtil.toIntUnsigned(x) / 64) | (1l << x);
            }
    }

    /**
     * Find the index of the next set bit greater or equal to i, returns -1 if
     * none found.
     * 
     * @param i
     *            starting index
     * @return index of the next set bit
     */
    public int nextSetBit(final int i) {
        int x = i >> 6; // signed i / 64
        long w = bitmap.get(x);
        w >>>= i;
        if (w != 0) {
            return i + Long.numberOfTrailingZeros(w);
        }
        for (++x; x < bitmap.limit(); ++x) {
            if (bitmap.get(x) != 0) {
                return x * 64 + Long.numberOfTrailingZeros(bitmap.get(x));
            }
        }
        return -1;
    }

    /**
     * Find the index of the previous set bit less than or equal to i, returns -1
     * if none found.
     *
     * @param i starting index
     * @return index of the previous set bit
     */
    public int prevSetBit(final int i) {
        int x = i >> 6; // signed i / 64
        long w = bitmap.get(x);
        w <<= 64 - i - 1;
        if (w != 0) {
            return i - Long.numberOfLeadingZeros(w);
        }
        for (--x; x >= 0; --x) {
            if (bitmap.get(x) != 0) {
                return x * 64 + 63 - Long.numberOfLeadingZeros(bitmap.get(x));
            }
        }
        return -1;
    }

    /**
     * Find the index of the next unset bit greater or equal to i, returns -1 if
     * none found.
     * 
     * @param i
     *            starting index
     * @return index of the next unset bit
     */
    public short nextUnsetBit(final int i) {
        int x = i / 64;
        long w = ~bitmap.get(x);
        w >>>= i;
        if (w != 0) {
            return (short) (i + Long.numberOfTrailingZeros(w));
        }
        ++x;
        for (; x < bitmap.limit(); ++x) {
            if (bitmap.get(x) != ~0L) {
                return (short) (x * 64 + Long.numberOfTrailingZeros(~bitmap
                        .get(x)));
            }
        }
        return -1;
    }

    @Override
    public MappeableContainer not(final int firstOfRange, final int lastOfRange) {
        return not(new MappeableBitmapContainer(), firstOfRange, lastOfRange);
    }

    // answer could be a new BitmapContainer, or (for inplace) it can be
    // "this"
    private MappeableContainer not(MappeableBitmapContainer answer,
            final int firstOfRange, final int lastOfRange) {
        // TODO: this can be optimized for performance
        assert bitmap.limit() == MAX_CAPACITY / 64; // checking
        // assumption
        // that partial
        // bitmaps are not
        // allowed
        // an easy case for full range, should be common
        if (lastOfRange - firstOfRange  == MAX_CAPACITY) {
            final int newCardinality = MAX_CAPACITY - cardinality;
            for (int k = 0; k < this.bitmap.limit(); ++k)
                answer.bitmap.put(k, ~this.bitmap.get(k));
            answer.cardinality = newCardinality;
            if (newCardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
                return answer.toArrayContainer();
            return answer;
        }

        // could be optimized to first determine the answer cardinality,
        // rather than update/create bitmap and then possibly convert

        int cardinalityChange = 0;
        final int rangeFirstWord = firstOfRange / 64;
        final int rangeFirstBitPos = firstOfRange & 63;
        final int rangeLastWord = (lastOfRange - 1) / 64;
        final long rangeLastBitPos = (lastOfRange - 1) & 63;

        // if not in place, we need to duplicate stuff before
        // rangeFirstWord and after rangeLastWord
        if (answer != this) {
            for (int i = 0; i < rangeFirstWord; ++i)
                answer.bitmap.put(i, bitmap.get(i));
            for (int i = rangeLastWord + 1; i < bitmap.limit(); ++i)
                answer.bitmap.put(i, bitmap.get(i));
        }

        // unfortunately, the simple expression gives the wrong mask for
        // rangeLastBitPos==63
        // no branchless way comes to mind
        final long maskOnLeft = (rangeLastBitPos == 63) ? -1L
                : (1L << (rangeLastBitPos + 1)) - 1;

        long mask = -1L; // now zero out stuff in the prefix
        mask ^= ((1L << rangeFirstBitPos) - 1);

        if (rangeFirstWord == rangeLastWord) {
            // range starts and ends in same word (may have
            // unchanged bits on both left and right)
            mask &= maskOnLeft;
            cardinalityChange = -Long.bitCount(bitmap.get(rangeFirstWord));
            answer.bitmap
                    .put(rangeFirstWord, bitmap.get(rangeFirstWord) ^ mask);
            cardinalityChange += Long.bitCount(answer.bitmap
                    .get(rangeFirstWord));
            answer.cardinality = cardinality + cardinalityChange;

            if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
                return answer.toArrayContainer();
            return answer;
        }

        // range spans words
        cardinalityChange += -Long.bitCount(bitmap.get(rangeFirstWord));
        answer.bitmap.put(rangeFirstWord, bitmap.get(rangeFirstWord) ^ mask);
        cardinalityChange += Long.bitCount(answer.bitmap.get(rangeFirstWord));

        cardinalityChange += -Long.bitCount(bitmap.get(rangeLastWord));
        answer.bitmap
                .put(rangeLastWord, bitmap.get(rangeLastWord) ^ maskOnLeft);
        cardinalityChange += Long.bitCount(answer.bitmap.get(rangeLastWord));

        // negate the words, if any, strictly between first and last
        for (int i = rangeFirstWord + 1; i < rangeLastWord; ++i) {
            cardinalityChange += (64 - 2 * Long.bitCount(bitmap.get(i)));
            answer.bitmap.put(i, ~bitmap.get(i));
        }
        answer.cardinality = cardinality + cardinalityChange;

        if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public MappeableBitmapContainer or(final MappeableArrayContainer value2) {

        final MappeableBitmapContainer answer = clone();
        long[] bitArray = answer.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(answer.bitmap) && BufferUtil.isBackedBySimpleArray(value2.content)) {
            long[] ab = answer.bitmap.array();
            short[] v2 = value2.content.array();
            for (int k = 0; k < value2.cardinality; ++k) {
                final int i = BufferUtil.toIntUnsigned(v2[k]) >>> 6;
                answer.cardinality += ((~ab[i]) & (1l << v2[k])) >>> v2[k];
                bitArray[i] |= (1l << value2.content.get(k));
            }

        } else
            for (int k = 0; k < value2.cardinality; ++k) {
                final int i = BufferUtil.toIntUnsigned(value2.content.get(k)) >>> 6;
                answer.cardinality += ((~answer.bitmap.get(i)) & (1l << value2.content
                        .get(k))) >>> value2.content.get(k);
                bitArray[i] |= (1l << value2.content.get(k));
            }
        return answer;
    }

    @Override
    public MappeableContainer or(final MappeableBitmapContainer value2) {
        if (USE_IN_PLACE) {
            final MappeableBitmapContainer value1 = this.clone();
            return value1.ior(value2);
        }
        final MappeableBitmapContainer answer = new MappeableBitmapContainer();
        long[] bitArray = answer.bitmap.array();
        answer.cardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
            long[] b = this.bitmap.array();
            long[] v2 = value2.bitmap.array();
            for (int k = 0; k < answer.bitmap.limit(); ++k) {
                bitArray[k] = b[k] | v2[k];
                answer.cardinality += Long.bitCount(bitArray[k]);
            }
        } else
            for (int k = 0; k < answer.bitmap.limit(); ++k) {
                bitArray[k] = this.bitmap.get(k) | value2.bitmap.get(k);
                answer.cardinality += Long.bitCount(bitArray[k]);
            }
        return answer;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        // little endian
        this.cardinality = 0;
        for (int k = 0; k < bitmap.limit(); ++k) {
            long w = Long.reverseBytes(in.readLong());
            bitmap.put(k,w);
            this.cardinality += Long.bitCount(w);
        }
    }

    @Override
    public MappeableContainer remove(final short i) {
        final int x = BufferUtil.toIntUnsigned(i);
        if (cardinality == MappeableArrayContainer.DEFAULT_MAX_SIZE + 1) {// this is
            // the
            // uncommon
            // path
            if ((bitmap.get(x / 64) & (1l << x)) != 0) {
                --cardinality;
                bitmap.put(x / 64, bitmap.get(x / 64) & ~(1l << x));
                return this.toArrayContainer();
            }
        }
        cardinality -= (bitmap.get(x / 64) & (1l << x)) >>> x;
        bitmap.put(x / 64, bitmap.get(x / 64) & ~(1l << x));
        return this;
    }

    /**
     * Copies the data to an array container
     * 
     * @return the array container
     */
    public MappeableArrayContainer toArrayContainer() {
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                cardinality);
        ac.loadData(this);
        return ac;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final ShortIterator i = this.getShortIterator();
        sb.append("{");
        while (i.hasNext()) {
            sb.append(i);
            if (i.hasNext())
                sb.append(",");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public void trim() {
    }

    @Override
    protected void writeArray(DataOutput out) throws IOException {
        // little endian
        for (int k = 0; k < bitmap.limit(); ++k) {
            final long w = bitmap.get(k);
            out.writeLong(Long.reverseBytes(w));
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        writeArray(out);
    }

    @Override
    public MappeableContainer xor(final MappeableArrayContainer value2) {
        final MappeableBitmapContainer answer = clone();
        long[] bitArray = answer.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(value2.content)) {
            short[] v2 = value2.content.array();
            for (int k = 0; k < value2.getCardinality(); ++k) {
                final int index = BufferUtil.toIntUnsigned(v2[k]) >>> 6;
                answer.cardinality += 1 - 2 * ((bitArray[index] & (1l << v2[k])) >>> v2[k]);
                bitArray[index] ^= (1l << v2[k]);
            }
        } else
            for (int k = 0; k < value2.getCardinality(); ++k) {
                final int index = BufferUtil.toIntUnsigned(value2.content
                        .get(k)) >>> 6;
                answer.cardinality += 1 - 2 * ((bitArray[index] & (1l << value2.content
                        .get(k))) >>> value2.content.get(k));
                bitArray[index] ^= (1l << value2.content.get(k));
            }
        if (answer.cardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public MappeableContainer xor(MappeableBitmapContainer value2) {

        int newCardinality = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
            long[] b = this.bitmap.array();
            long[] v2 = value2.bitmap.array();
            for (int k = 0; k < this.bitmap.limit(); ++k) {

                newCardinality += Long.bitCount(b[k] ^ v2[k]);

            }
        } else

            for (int k = 0; k < this.bitmap.limit(); ++k) {
                newCardinality += Long.bitCount(this.bitmap.get(k)
                        ^ value2.bitmap.get(k));
            }
        if (newCardinality > MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            final MappeableBitmapContainer answer = new MappeableBitmapContainer();
            long[] bitArray = answer.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap)) {
                long[] b = this.bitmap.array();
                long[] v2 = value2.bitmap.array();
                for (int k = 0; k < answer.bitmap.limit(); ++k) {
                    bitArray[k] = b[k] ^ v2[k];
                }
            } else
                for (int k = 0; k < answer.bitmap.limit(); ++k) {
                    bitArray[k] = this.bitmap.get(k) ^ value2.bitmap.get(k);
                }
            answer.cardinality = newCardinality;
            return answer;
        }
        final MappeableArrayContainer ac = new MappeableArrayContainer(
                newCardinality);
        if (BufferUtil.isBackedBySimpleArray(this.bitmap) && BufferUtil.isBackedBySimpleArray(value2.bitmap))
            org.roaringbitmap.Util.fillArrayXOR(ac.content.array(),
                    this.bitmap.array(), value2.bitmap.array());
        else

            BufferUtil.fillArrayXOR(ac.content.array(), this.bitmap,
                    value2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }
 
    protected MappeableContainer ilazyor(MappeableArrayContainer value2) {
        this.cardinality = -1;// invalid
        long[] b = this.bitmap.array();
        for (int k = 0; k < value2.cardinality; ++k) {
            final int i = BufferUtil.toIntUnsigned(value2.content.get(k)) >>> 6;
            b[i] |= (1l << value2.content.get(k));
        }
        return this;
    }

    protected MappeableContainer ilazyor(MappeableBitmapContainer x) {
        this.cardinality = -1;// invalid
        long[] b = this.bitmap.array();
        if (BufferUtil.isBackedBySimpleArray(x.bitmap)) {
            long[] b2 = x.bitmap.array();
            for (int k = 0; k < b.length; k++) {
                b[k] |= b2[k];
            }
        } else {
            for (int k = 0; k < b.length; k++) {
                b[k] |= x.bitmap.get(k);
            }
        }
        return this;
    }
    
    protected MappeableContainer lazyor(MappeableArrayContainer value2) {
        MappeableBitmapContainer answer = clone();
        answer.cardinality = -1;// invalid
        long[] b = answer.bitmap.array();
        for (int k = 0; k < value2.cardinality; ++k) {
            final int i = BufferUtil.toIntUnsigned(value2.content.get(k)) >>> 6;
            b[i] |=  1l << value2.content.get(k);
        }
        return answer;
    }

    protected MappeableContainer lazyor(MappeableBitmapContainer x) {
        MappeableBitmapContainer answer = new MappeableBitmapContainer();
        answer.cardinality = -1;// invalid
        long[] b = answer.bitmap.array();
        for (int k = 0; k < b.length; k++) {
            b[k] = this.bitmap.get(k) | x.bitmap.get(k);
        }
        return answer;
    }    
    
    protected void computeCardinality() {
        this.cardinality = 0;
        long[] b = this.bitmap.array();
        for (int k = 0; k < b.length; k++) {
            this.cardinality += Long.bitCount(b[k]);
        }
    }


    @Override
    public int rank(short lowbits) {
        int x = BufferUtil.toIntUnsigned(lowbits);
        int leftover = (x + 1) & 63;
        int answer = 0;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            long[] b = this.bitmap.array();
            for (int k = 0; k < (x + 1) / 64; ++k)
                answer += Long.bitCount(b[k]);
            if (leftover != 0) {
                answer += Long.bitCount(b[(x + 1) / 64] << (64 - leftover));
            }
        } else {
            for (int k = 0; k < (x + 1) / 64; ++k)
                answer += Long.bitCount(bitmap.get(k));
            if (leftover != 0) {
                answer += Long
                        .bitCount(bitmap.get((x + 1) / 64) << (64 - leftover));
            }
        }
        return answer;
    }

    @Override
    public short select(int j) {
        int leftover = j;
        if (BufferUtil.isBackedBySimpleArray(this.bitmap)) {
            long[] b = this.bitmap.array();

            for (int k = 0; k < b.length; ++k) {
                int w = Long.bitCount(b[k]);
                if (w > leftover) {
                    return (short) (k * 64 + Util.select(b[k], leftover));
                }
                leftover -= w;
            }
        } else {

            for (int k = 0; k < bitmap.limit(); ++k) {
                int w = Long.bitCount(bitmap.get(k));
                if (w > leftover) {
                    return (short) (k * 64 + Util.select(bitmap.get(k),
                            leftover));
                }
                leftover -= w;
            }
        }
        throw new IllegalArgumentException("Insufficient cardinality.");
    }

    @Override
    public MappeableContainer limit(int maxcardinality) {
        if(maxcardinality >= this.cardinality) {
            return clone();
        } 
        if(maxcardinality <= MappeableArrayContainer.DEFAULT_MAX_SIZE) {
            MappeableArrayContainer ac = new MappeableArrayContainer(maxcardinality);
            int pos = 0;
            short[] cont = ac.content.array();
            for (int k = 0; (ac.cardinality <maxcardinality) && (k < bitmap.limit()); ++k) {
                long bitset = bitmap.get(k);
                while ((ac.cardinality <maxcardinality) && ( bitset != 0)) {
                    long t = bitset & -bitset;
                    cont[pos++] = (short) (k * 64 + Long
                            .bitCount(t - 1));
                    ac.cardinality++;
                    bitset ^= t;
                }
            }
            return ac;
        } 
        MappeableBitmapContainer bc = new MappeableBitmapContainer(maxcardinality,this.bitmap);
        int s = BufferUtil.toIntUnsigned(select(maxcardinality));
        int usedwords = (s+63)/64;
        int todelete = this.bitmap.limit() - usedwords;
        for(int k = 0; k<todelete; ++k)
            bc.bitmap.put(bc.bitmap.limit()-1-k, 0);
        int lastword = s % 64; 
        if(lastword != 0) {
            bc.bitmap.put(s/64,  (bc.bitmap.get(s/64) << (64-lastword)) >> (64-lastword));
        }
        return bc;
    }

    @Override
    public MappeableContainer flip(short i) {
        final int x = BufferUtil.toIntUnsigned(i);
        if (cardinality == MappeableArrayContainer.DEFAULT_MAX_SIZE + 1) {// this
                                                                          // is
            // the
            // uncommon
            // path
            if ((bitmap.get(x / 64) & (1l << x)) != 0) {
                --cardinality;
                bitmap.put(x / 64, bitmap.get(x / 64) & ~(1l << x));
                return this.toArrayContainer();
            }
        }
        cardinality += 1 - 2 * ((bitmap.get(x / 64) & (1l << x)) >>> x);
        bitmap.put(x / 64, bitmap.get(x / 64) ^ (1l << x));
        return this;
    }

    @Override
    public MappeableContainer iadd(int begin, int end) {
        BufferUtil.setBitmapRange(bitmap, begin, end);
        computeCardinality();
        return this;
    }

    @Override
    public MappeableContainer iremove(int begin, int end) {
        BufferUtil.resetBitmapRange(bitmap, begin, end);
        computeCardinality();
        if (getCardinality() < MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return toArrayContainer();
        return this;
    }

    @Override
    public MappeableContainer add(int begin, int end) {
        MappeableBitmapContainer answer = clone();
        BufferUtil.setBitmapRange(answer.bitmap, begin, end);
        answer.computeCardinality();
        return answer;
    }

    @Override
    public MappeableContainer remove(int begin, int end) {
        MappeableBitmapContainer answer = clone();
        BufferUtil.resetBitmapRange(answer.bitmap, begin, end);
        answer.computeCardinality();
        if (answer.getCardinality() < MappeableArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }
}

final class MappeableBitmapContainerShortIterator implements ShortIterator {
    int i;
    
    MappeableBitmapContainer parent;

    MappeableBitmapContainerShortIterator() {
    }

    MappeableBitmapContainerShortIterator(MappeableBitmapContainer p) {
        wrap(p);
    }

    void wrap(MappeableBitmapContainer p) {
        parent = p;
        i = parent.nextSetBit(0);
    }

    @Override
    public boolean hasNext() {
        return i >= 0;
    }

    @Override
    public short next() {
        final int j = i;
        i = i + 1 < parent.bitmap.limit() * 64 ? parent.nextSetBit(i + 1) : -1;
        return (short) j;
    }

    @Override
    public ShortIterator clone() {
        try {
            return (ShortIterator) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;// will not happen
        }
    }

    @Override
    public void remove() {
        //TODO: implement
        throw new RuntimeException("unsupported operation: remove");
    }
}


final class ReverseMappeableBitmapContainerShortIterator implements ShortIterator {
    
    int i;

    MappeableBitmapContainer parent;
    
    ReverseMappeableBitmapContainerShortIterator() {
    }
    
    ReverseMappeableBitmapContainerShortIterator(MappeableBitmapContainer p) {
        wrap(p);
    }

    public void wrap(MappeableBitmapContainer p) {
        parent = p;
        i = parent.prevSetBit(parent.bitmap.limit() * 64 - 1);
    }

    @Override
    public boolean hasNext() {
        return i >= 0;
    }

    @Override
    public short next() {
        final int j = i;
        i = i > 0 ? parent.prevSetBit(i - 1) : -1;
        return (short) j;
    }

    @Override
    public ShortIterator clone() {
        try {
            return (ShortIterator) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    @Override
    public void remove() {
        //TODO: implement
        throw new RuntimeException("unsupported operation: remove");
    }
}