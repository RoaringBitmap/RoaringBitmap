/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;

import org.roaringbitmap.ShortIterator;


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
     *            last index (range is inclusive)
     */
    public MappeableBitmapContainer(final int firstOfRun, final int lastOfRun) {
        // TODO: this can be optimized for performance
        this.cardinality = lastOfRun - firstOfRun + 1;
        this.bitmap = LongBuffer.allocate(MAX_CAPACITY / 64);
        if (this.cardinality == MAX_CAPACITY) {// perhaps a common case
            for (int k = 0; k < bitmap.limit(); ++k)
                bitmap.put(k, -1L);
        } else {
            final int firstWord = firstOfRun / 64;
            final int lastWord = lastOfRun / 64;
            final int zeroPrefixLength = firstOfRun & 63;
            final int zeroSuffixLength = 63 - (lastOfRun & 63);
            for (int k = firstWord; k < lastWord + 1; ++k)
                bitmap.put(k, -1L);
            bitmap.put(firstWord, bitmap.get(firstWord)
                    ^ ((1L << zeroPrefixLength) - 1));
            final long blockOfOnes = (1L << zeroSuffixLength) - 1;
            final long maskOnLeft = blockOfOnes << (64 - zeroSuffixLength);
            bitmap.put(lastWord, bitmap.get(lastWord) ^ maskOnLeft);
        }
    }

    private MappeableBitmapContainer(int newCardinality, LongBuffer newBitmap) {
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
        if (value2.content.hasArray()) {
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
        if (this.bitmap.hasArray() && value2.bitmap.hasArray()) {
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
            if (this.bitmap.hasArray() && value2.bitmap.hasArray()) {
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
        if (this.bitmap.hasArray() && value2.bitmap.hasArray())
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
        if (value2.content.hasArray() && this.bitmap.hasArray()) {
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
        if (this.bitmap.hasArray() && value2.bitmap.hasArray()) {
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
            if (this.bitmap.hasArray() && value2.bitmap.hasArray()) {
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
        if (this.bitmap.hasArray() && value2.bitmap.hasArray())
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
            if (this.bitmap.hasArray() && srb.bitmap.hasArray()) {
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
        if (bitmap.hasArray()) {
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
        if (bitmap.hasArray()) {
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
        return new ShortIterator() {
            int i = MappeableBitmapContainer.this.nextSetBit(0);

            int j;

            @Override
            public boolean hasNext() {
                return i >= 0;
            }

            @Override
            public short next() {
                j = i;
                i = MappeableBitmapContainer.this.nextSetBit(i + 1);
                return (short) j;
            }

            
        };
    }

    @Override
    public int getSizeInBytes() {
        return this.bitmap.limit() * 8;
    }

    @Override
    public int hashCode() {
        long hash = 0;
        for (int k = 0; k < this.bitmap.limit(); ++k)
            hash += 31 * this.bitmap.get(k);
        return (int) (hash >> 32);
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
        if (b2.bitmap.hasArray()) {
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

    // complicated so that it should be reasonably efficient even when the
    // ranges are small
    @Override
    public MappeableContainer inot(final int firstOfRange, final int lastOfRange) {
        return not(this, firstOfRange, lastOfRange);
    }

    @Override
    public MappeableBitmapContainer ior(final MappeableArrayContainer value2) {
        long[] b = this.bitmap.array();
        if (value2.content.hasArray()) {

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
        if (b2.bitmap.hasArray()) {
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
            int i = MappeableBitmapContainer.this.nextSetBit(0);

            int j;

            @Override
            public boolean hasNext() {
                return i >= 0;
            }

            @Override
            public Short next() {
                j = i;
                i = MappeableBitmapContainer.this.nextSetBit(i + 1);
                return (short) j;
            }

            @Override
            public void remove() {
                throw new RuntimeException("unsupported operation: remove");
            }

        };
    }

    @Override
    public MappeableContainer ixor(final MappeableArrayContainer value2) {
        long[] b = bitmap.array();
        if (value2.content.hasArray()) {
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
        if (b2.bitmap.hasArray()) {
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
        if (bitmap.hasArray() && arrayContainer.content.hasArray()) {
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
        int x = i / 64;
        if (x >= bitmap.limit())
            return -1;
        long w = bitmap.get(x);
        w >>>= i;
        if (w != 0) {
            return i + Long.numberOfTrailingZeros(w);
        }
        ++x;
        for (; x < bitmap.limit(); ++x) {
            if (bitmap.get(x) != 0) {
                return x * 64 + Long.numberOfTrailingZeros(bitmap.get(x));
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
        if (lastOfRange - firstOfRange + 1 == MAX_CAPACITY) {
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
        final int rangeLastWord = lastOfRange / 64;
        final long rangeLastBitPos = lastOfRange & 63;

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
        if (answer.bitmap.hasArray() && value2.content.hasArray()) {
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
        if (this.bitmap.hasArray() && value2.bitmap.hasArray()) {
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
        final byte[] buffer = new byte[8];
        // little endian
        this.cardinality = 0;
        for (int k = 0; k < bitmap.limit(); ++k) {
            in.readFully(buffer);
            bitmap.put(k,
                    (((long) buffer[7] << 56)
                            + ((long) (buffer[6] & 255) << 48)
                            + ((long) (buffer[5] & 255) << 40)
                            + ((long) (buffer[4] & 255) << 32)
                            + ((long) (buffer[3] & 255) << 24)
                            + ((buffer[2] & 255) << 16)
                            + ((buffer[1] & 255) << 8) + (buffer[0] & 255)));
            this.cardinality += Long.bitCount(bitmap.get(k));
        }
    }

    @Override
    public MappeableContainer remove(final short i) {
        final int x = BufferUtil.toIntUnsigned(i);
        if (cardinality == MappeableArrayContainer.DEFAULT_MAX_SIZE) {// this is
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
        sb.append("{");
        int i = this.nextSetBit(0);
        while (i >= 0) {
            sb.append(i);
            i = this.nextSetBit(i + 1);
            if (i >= 0)
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

        final byte[] buffer = new byte[8];
        // little endian
        for (int k = 0; k < MAX_CAPACITY / 64; ++k) {
            final long w = bitmap.get(k);
            buffer[0] = (byte) w;
            buffer[1] = (byte) (w >>> 8);
            buffer[2] = (byte) (w >>> 16);
            buffer[3] = (byte) (w >>> 24);
            buffer[4] = (byte) (w >>> 32);
            buffer[5] = (byte) (w >>> 40);
            buffer[6] = (byte) (w >>> 48);
            buffer[7] = (byte) (w >>> 56);
            out.write(buffer, 0, 8);
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
        if (value2.content.hasArray()) {
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
        if (this.bitmap.hasArray() && value2.bitmap.hasArray()) {
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
            if (this.bitmap.hasArray() && value2.bitmap.hasArray()) {
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
        if (this.bitmap.hasArray() && value2.bitmap.hasArray())
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
        if (x.bitmap.hasArray()) {
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

}
