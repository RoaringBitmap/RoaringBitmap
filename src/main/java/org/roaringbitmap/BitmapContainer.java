/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Simple bitset-like container.
 */
public final class BitmapContainer extends Container implements Cloneable, Serializable {
    protected static final int MAX_CAPACITY = 1 << 16;

    private static final long serialVersionUID = 2L;

    private static boolean USE_IN_PLACE = true; // optimization flag

    long[] bitmap;

    int cardinality;

    /**
     * Create a bitmap container with all bits set to false
     */
    public BitmapContainer() {
        this.cardinality = 0;
        this.bitmap = new long[MAX_CAPACITY / 64];
    }

    /**
     * Create a bitmap container with a run of ones from firstOfRun to
     * lastOfRun, inclusive caller must ensure that the range isn't so small
     * that an ArrayContainer should have been created instead
     *
     * @param firstOfRun first index
     * @param lastOfRun  last index (range is inclusive)
     */
    public BitmapContainer(final int firstOfRun, final int lastOfRun) {
        this.cardinality = lastOfRun - firstOfRun + 1;
        this.bitmap = new long[MAX_CAPACITY / 64];
        if (this.cardinality == MAX_CAPACITY) // perhaps a common case
            Arrays.fill(bitmap, -1L);
        else {
            final int firstWord = firstOfRun / 64;
            final int lastWord = lastOfRun / 64;
            final int zeroPrefixLength = firstOfRun & 63;
            final int zeroSuffixLength = 63 - (lastOfRun & 63);

            Arrays.fill(bitmap, firstWord, lastWord + 1, -1L);
            bitmap[firstWord] ^= ((1L << zeroPrefixLength) - 1);
            final long blockOfOnes = (1L << zeroSuffixLength) - 1;
            final long maskOnLeft = blockOfOnes << (64 - zeroSuffixLength);
            bitmap[lastWord] ^= maskOnLeft;
        }
    }

    private BitmapContainer(int newCardinality, long[] newBitmap) {
        this.cardinality = newCardinality;
        this.bitmap = Arrays.copyOf(newBitmap, newBitmap.length);
    }

    protected BitmapContainer(long[] newBitmap, int newCardinality) {
        this.cardinality = newCardinality;
        this.bitmap = newBitmap;
    }

    @Override
    public Container add(final short i) {
        final int x = Util.toIntUnsigned(i);
        final long previous = bitmap[x / 64];
        bitmap[x / 64] |= (1l << x);
        cardinality += (previous ^ bitmap[x / 64]) >>> x;
        return this;
    }

    @Override
    public ArrayContainer and(final ArrayContainer value2) {
        final ArrayContainer answer = new ArrayContainer(value2.content.length);
        for (int k = 0; k < value2.getCardinality(); ++k)
            if (this.contains(value2.content[k]))
                answer.content[answer.cardinality++] = value2.content[k];
        return answer;
    }

    @Override
    public Container and(final BitmapContainer value2) {
        int newCardinality = 0;
        for (int k = 0; k < this.bitmap.length; ++k) {
            newCardinality += Long.bitCount(this.bitmap[k]
                    & value2.bitmap[k]);
        }
        if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
            final BitmapContainer answer = new BitmapContainer();
            for (int k = 0; k < answer.bitmap.length; ++k) {
                answer.bitmap[k] = this.bitmap[k]
                        & value2.bitmap[k];
            }
            answer.cardinality = newCardinality;
            return answer;
        }
        ArrayContainer ac = new ArrayContainer(newCardinality);
        Util.fillArrayAND(ac.content, this.bitmap, value2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    @Override
    public Container andNot(final ArrayContainer value2) {
        final BitmapContainer answer = clone();
        for (int k = 0; k < value2.cardinality; ++k) {
            final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
            answer.bitmap[i] = answer.bitmap[i]
                    & (~(1l << value2.content[k]));
            answer.cardinality -= (answer.bitmap[i] ^ this.bitmap[i]) >>> value2.content[k];
        }
        if (answer.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public Container andNot(final BitmapContainer value2) {
        int newCardinality = 0;
        for (int k = 0; k < this.bitmap.length; ++k) {
            newCardinality += Long.bitCount(this.bitmap[k]
                    & (~value2.bitmap[k]));
        }
        if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
            final BitmapContainer answer = new BitmapContainer();
            for (int k = 0; k < answer.bitmap.length; ++k) {
                answer.bitmap[k] = this.bitmap[k]
                        & (~value2.bitmap[k]);
            }
            answer.cardinality = newCardinality;
            return answer;
        }
        ArrayContainer ac = new ArrayContainer(newCardinality);
        Util.fillArrayANDNOT(ac.content, this.bitmap, value2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    @Override
    public void clear() {
        if (cardinality != 0) {
            cardinality = 0;
            Arrays.fill(bitmap, 0);
        }
    }

    @Override
    public BitmapContainer clone() {
        return new BitmapContainer(this.cardinality, this.bitmap);
    }

    @Override
    public boolean contains(final short i) {
        final int x = Util.toIntUnsigned(i);
        return (bitmap[x / 64] & (1l << x)) != 0;
    }

    @Override
    public void deserialize(DataInput in) throws IOException {
        byte[] buffer = new byte[8];
        // little endian
        this.cardinality = 0;
        for (int k = 0; k < bitmap.length; ++k) {
            in.readFully(buffer);
            bitmap[k] = (((long) buffer[7] << 56)
                    + ((long) (buffer[6] & 255) << 48)
                    + ((long) (buffer[5] & 255) << 40)
                    + ((long) (buffer[4] & 255) << 32)
                    + ((long) (buffer[3] & 255) << 24)
                    + ((buffer[2] & 255) << 16)
                    + ((buffer[1] & 255) << 8)
                    + (buffer[0] & 255));
            this.cardinality += Long.bitCount(bitmap[k]);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BitmapContainer) {
            BitmapContainer srb = (BitmapContainer) o;
            if (srb.cardinality != this.cardinality)
                return false;
            return Arrays.equals(this.bitmap, srb.bitmap);
        }
        return false;
    }

    /**
     * Fill the array with set bits
     *
     * @param array container (should be sufficiently large)
     */
    protected void fillArray(final int[] array) {
        int pos = 0;
        for (int k = 0; k < bitmap.length; ++k) {
            long bitset = bitmap[k];
            while (bitset != 0) {
                long t = bitset & -bitset;
                array[pos++] = k * 64 + Long.bitCount(t - 1);
                bitset ^= t;
            }
        }
    }

    /**
     * Fill the array with set bits
     *
     * @param array container (should be sufficiently large)
     */
    protected void fillArray(final short[] array) {
        int pos = 0;
        for (int k = 0; k < bitmap.length; ++k) {
            long bitset = bitmap[k];
            while (bitset != 0) {
                long t = bitset & -bitset;
                array[pos++] = (short) (k * 64 + Long
                        .bitCount(t - 1));
                bitset ^= t;
            }
        }
    }

    @Override
    public void fillLeastSignificant16bits(int[] x, int i, int mask) {
        int pos = i;
        for (int k = 0; k < bitmap.length; ++k) {
            long bitset = bitmap[k];
            while (bitset != 0) {
                long t = bitset & -bitset;
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
            int i = BitmapContainer.this.nextSetBit(0);
            int j;

            @Override
            public boolean hasNext() {
                return i >= 0;
            }

            @Override
            public short next() {
                j = i;
                i = BitmapContainer.this.nextSetBit(i + 1);
                return (short) j;
            }

        };

    }

    @Override
    public int getSizeInBytes() {
        return this.bitmap.length * 8;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.bitmap);
    }

    @Override
    public Container iand(final ArrayContainer b2) {
        return b2.and(this);// no inplace possible
    }

    @Override
    public Container iand(final BitmapContainer b2) {
        int newCardinality = 0;
        for (int k = 0; k < this.bitmap.length; ++k) {
            newCardinality += Long.bitCount(this.bitmap[k] & b2.bitmap[k]);
        }
        if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
            for (int k = 0; k < this.bitmap.length; ++k) {
                this.bitmap[k] = this.bitmap[k] & b2.bitmap[k];
            }
            this.cardinality = newCardinality;
            return this;
        }
        ArrayContainer ac = new ArrayContainer(newCardinality);
        Util.fillArrayAND(ac.content, this.bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    @Override
    public Container iandNot(final ArrayContainer b2) {
        for (int k = 0; k < b2.cardinality; ++k) {
            this.remove(b2.content[k]);
        }
        if (cardinality <= ArrayContainer.DEFAULT_MAX_SIZE)
            return this.toArrayContainer();
        return this;
    }

    @Override
    public Container iandNot(final BitmapContainer b2) {
        int newCardinality = 0;
        for (int k = 0; k < this.bitmap.length; ++k) {
            newCardinality += Long.bitCount(this.bitmap[k] & (~b2.bitmap[k]));
        }
        if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
            for (int k = 0; k < this.bitmap.length; ++k) {
                this.bitmap[k] = this.bitmap[k] & (~b2.bitmap[k]);
            }
            this.cardinality = newCardinality;
            return this;
        }
        ArrayContainer ac = new ArrayContainer(newCardinality);
        Util.fillArrayANDNOT(ac.content, this.bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    // complicated so that it should be reasonably efficient even when the
    // ranges are small
    @Override
    public Container inot(final int firstOfRange, final int lastOfRange) {
        return not(this, firstOfRange, lastOfRange);
    }

    @Override
    public BitmapContainer ior(final ArrayContainer value2) {
        for (int k = 0; k < value2.cardinality; ++k) {
            final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
            this.cardinality += ((~this.bitmap[i]) & (1l << value2.content[k])) >>> value2.content[k];
            this.bitmap[i] |= (1l << value2.content[k]);
        }
        return this;
    }

    @Override
    public Container ior(final BitmapContainer b2) {
        this.cardinality = 0;
        for (int k = 0; k < this.bitmap.length; k++) {
            this.bitmap[k] |= b2.bitmap[k];
            this.cardinality += Long.bitCount(this.bitmap[k]);
        }
        return this;
    }

    @Override
    public Iterator<Short> iterator() {
        return new Iterator<Short>() {
            final ShortIterator si = BitmapContainer.this.getShortIterator();

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
                throw new RuntimeException("unsupported operation: remove");
            }
        };
    }

    @Override
    public Container ixor(final ArrayContainer value2) {
        for (int k = 0; k < value2.getCardinality(); ++k) {
            final int index = Util.toIntUnsigned(value2.content[k]) >>> 6;
            this.cardinality += 1 - 2 * ((this.bitmap[index] & (1l << value2.content[k])) >>> value2.content[k]);
            this.bitmap[index] ^= (1l << value2.content[k]);
        }
        if (this.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
            return this.toArrayContainer();
        }
        return this;
    }

    @Override
    public Container ixor(BitmapContainer b2) {
        int newCardinality = 0;
        for (int k = 0; k < this.bitmap.length; ++k) {
            newCardinality += Long.bitCount(this.bitmap[k] ^ b2.bitmap[k]);
        }
        if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
            for (int k = 0; k < this.bitmap.length; ++k) {
                this.bitmap[k] = this.bitmap[k] ^ b2.bitmap[k];
            }
            this.cardinality = newCardinality;
            return this;
        }
        ArrayContainer ac = new ArrayContainer(newCardinality);
        Util.fillArrayXOR(ac.content, this.bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    protected void loadData(final ArrayContainer arrayContainer) {
        this.cardinality = arrayContainer.cardinality;
        for (int k = 0; k < arrayContainer.cardinality; ++k) {
            final short x = arrayContainer.content[k];
            bitmap[Util.toIntUnsigned(x) / 64] |= (1l << x);
        }
    }

    /**
     * Find the index of the next set bit greater or equal to i, returns -1
     * if none found.
     *
     * @param i starting index
     * @return index of the next set bit
     */
    public int nextSetBit(final int i) {
        int x = i / 64;
        if (x >= bitmap.length)
            return -1;
        long w = bitmap[x];
        w >>>= i;
        if (w != 0) {
            return i + Long.numberOfTrailingZeros(w);
        }
        ++x;
        for (; x < bitmap.length; ++x) {
            if (bitmap[x] != 0) {
                return x * 64 + Long.numberOfTrailingZeros(bitmap[x]);
            }
        }
        return -1;
    }

    /**
     * Find the index of the next unset bit greater or equal to i, returns
     * -1 if none found.
     *
     * @param i starting index
     * @return index of the next unset bit
     */
    public short nextUnsetBit(final int i) {
        int x = i / 64;
        long w = ~bitmap[x];
        w >>>= i;
        if (w != 0) {
            return (short) (i + Long.numberOfTrailingZeros(w));
        }
        ++x;
        for (; x < bitmap.length; ++x) {
            if (bitmap[x] != ~0L) {
                return (short) (x * 64 + Long.numberOfTrailingZeros(~bitmap[x]));
            }
        }
        return -1;
    }

    // answer could be a new BitmapContainer, or (for inplace) it can be
    // "this"
    private Container not(BitmapContainer answer, final int firstOfRange,
                          final int lastOfRange) {
        assert bitmap.length == MAX_CAPACITY / 64; // checking assumption
        // that partial
        // bitmaps are not
        // allowed
        // an easy case for full range, should be common
        if (lastOfRange - firstOfRange + 1 == MAX_CAPACITY) {
            final int newCardinality = MAX_CAPACITY - cardinality;
            for (int k = 0; k < this.bitmap.length; ++k)
                answer.bitmap[k] = ~this.bitmap[k];
            answer.cardinality = newCardinality;
            if (newCardinality <= ArrayContainer.DEFAULT_MAX_SIZE)
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
            System.arraycopy(bitmap, 0, answer.bitmap, 0, rangeFirstWord);
            System.arraycopy(bitmap, rangeLastWord + 1, answer.bitmap,
                    rangeLastWord + 1, bitmap.length - (rangeLastWord + 1));
        }

        // unfortunately, the simple expression gives the wrong mask for
        // rangeLastBitPos==63
        // no branchless way comes to mind
        final long maskOnLeft = (rangeLastBitPos == 63) ? -1L : (1L << (rangeLastBitPos + 1)) - 1;

        long mask = -1L; // now zero out stuff in the prefix
        mask ^= ((1L << rangeFirstBitPos) - 1);

        if (rangeFirstWord == rangeLastWord) {
            // range starts and ends in same word (may have
            // unchanged bits on both left and right)
            mask &= maskOnLeft;
            cardinalityChange = -Long.bitCount(bitmap[rangeFirstWord]);
            answer.bitmap[rangeFirstWord] = bitmap[rangeFirstWord] ^ mask;
            cardinalityChange += Long.bitCount(answer.bitmap[rangeFirstWord]);
            answer.cardinality = cardinality + cardinalityChange;

            if (answer.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE)
                return answer.toArrayContainer();
            return answer;
        }

        // range spans words
        cardinalityChange += -Long.bitCount(bitmap[rangeFirstWord]);
        answer.bitmap[rangeFirstWord] = bitmap[rangeFirstWord] ^ mask;
        cardinalityChange += Long.bitCount(answer.bitmap[rangeFirstWord]);

        cardinalityChange += -Long.bitCount(bitmap[rangeLastWord]);
        answer.bitmap[rangeLastWord] = bitmap[rangeLastWord] ^ maskOnLeft;
        cardinalityChange += Long.bitCount(answer.bitmap[rangeLastWord]);

        // negate the words, if any, strictly between first and last
        for (int i = rangeFirstWord + 1; i < rangeLastWord; ++i) {
            cardinalityChange += (64 - 2 * Long.bitCount(bitmap[i]));
            answer.bitmap[i] = ~bitmap[i];
        }
        answer.cardinality = cardinality + cardinalityChange;

        if (answer.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public Container not(final int firstOfRange, final int lastOfRange) {
        return not(new BitmapContainer(), firstOfRange, lastOfRange);
    }

    @Override
    public BitmapContainer or(final ArrayContainer value2) {
        final BitmapContainer answer = clone();
        for (int k = 0; k < value2.cardinality; ++k) {
            final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
            answer.cardinality += ((~answer.bitmap[i]) & (1l << value2.content[k])) >>> value2.content[k];
            answer.bitmap[i] = answer.bitmap[i] | (1l << value2.content[k]);
        }
        return answer;
    }

    @Override
    public Container or(final BitmapContainer value2) {
        if (USE_IN_PLACE) {
            BitmapContainer value1 = this.clone();
            return value1.ior(value2);
        }
        final BitmapContainer answer = new BitmapContainer();
        answer.cardinality = 0;
        for (int k = 0; k < answer.bitmap.length; ++k) {
            answer.bitmap[k] = this.bitmap[k] | value2.bitmap[k];
            answer.cardinality += Long.bitCount(answer.bitmap[k]);
        }
        return answer;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public Container remove(final short i) {
        final int x = Util.toIntUnsigned(i);
        if (cardinality == ArrayContainer.DEFAULT_MAX_SIZE) {// this is
            // the
            // uncommon
            // path
            if ((bitmap[x / 64] & (1l << x)) != 0) {
                --cardinality;
                bitmap[x / 64] &= ~(1l << x);
                return this.toArrayContainer();
            }
        }
        cardinality -= (bitmap[x / 64] & (1l << x)) >>> x;
        bitmap[x / 64] &= ~(1l << x);
        return this;
    }

    @Override
    public void serialize(DataOutput out) throws IOException {
        byte[] buffer = new byte[8];
        // little endian
        for (long w : bitmap) {
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
    public int serializedSizeInBytes() {
        return MAX_CAPACITY / 8;
    }

    /**
     * Copies the data to an array container
     *
     * @return the array container
     */
    public ArrayContainer toArrayContainer() {
        ArrayContainer ac = new ArrayContainer(cardinality);
        ac.loadData(this);
        return ac;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
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
            final long w = bitmap[k];
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
        serialize(out);
    }

    @Override
    public Container xor(final ArrayContainer value2) {
        final BitmapContainer answer = clone();
        for (int k = 0; k < value2.getCardinality(); ++k) {
            final int index = Util.toIntUnsigned(value2.content[k]) >>> 6;
            answer.cardinality += 1 - 2 * ((answer.bitmap[index] & (1l << value2.content[k])) >>> value2.content[k]);
            answer.bitmap[index] = answer.bitmap[index]
                    ^ (1l << value2.content[k]);
        }
        if (answer.cardinality <= ArrayContainer.DEFAULT_MAX_SIZE)
            return answer.toArrayContainer();
        return answer;
    }

    @Override
    public Container xor(BitmapContainer value2) {
        int newCardinality = 0;
        for (int k = 0; k < this.bitmap.length; ++k) {
            newCardinality += Long.bitCount(this.bitmap[k] ^ value2.bitmap[k]);
        }
        if (newCardinality > ArrayContainer.DEFAULT_MAX_SIZE) {
            final BitmapContainer answer = new BitmapContainer();
            for (int k = 0; k < answer.bitmap.length; ++k) {
                answer.bitmap[k] = this.bitmap[k]^ value2.bitmap[k];
            }
            answer.cardinality = newCardinality;
            return answer;
        }
        ArrayContainer ac = new ArrayContainer(newCardinality);
        Util.fillArrayXOR(ac.content, this.bitmap, value2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    protected Container ilazyor(ArrayContainer value2) {
        this.cardinality = -1;// invalid
        for (int k = 0; k < value2.cardinality; ++k) {
            final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
            this.bitmap[i] |= (1l << value2.content[k]);
        }
        return this;
    }

    protected Container ilazyor(BitmapContainer x) {
        this.cardinality = -1;// invalid
        for (int k = 0; k < this.bitmap.length; k++) {
            this.bitmap[k] |= x.bitmap[k];
        }
        return this;
    }
    
    protected Container lazyor(ArrayContainer value2) {
        BitmapContainer answer = this.clone();
        answer.cardinality = -1;// invalid
        for (int k = 0; k < value2.cardinality; ++k) {
            final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
            answer.bitmap[i] |= (1l << value2.content[k]);
        }
        return answer;
    }

    protected Container lazyor(BitmapContainer x) {
        BitmapContainer answer = new BitmapContainer();
        answer.cardinality = -1;// invalid
        for (int k = 0; k < this.bitmap.length; k++) {
            answer.bitmap[k] = this.bitmap[k] | x.bitmap[k];
        }
        return answer;
    }    
    
    protected void computeCardinality() {
        this.cardinality = 0;
        for (int k = 0; k < this.bitmap.length; k++) {
            this.cardinality += Long.bitCount(this.bitmap[k]);
        }
    }
}
