/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Simple container made of an array of 16-bit integers
 */
public final class ArrayContainer extends Container implements Cloneable, Serializable {
    private static final int DEFAULT_INIT_SIZE = 4;

    static final int DEFAULT_MAX_SIZE = 4096;

    private static final long serialVersionUID = 1L;

    protected int cardinality = 0;

    short[] content;

    /**
     * Create an array container with default capacity
     */
    public ArrayContainer() {
        this(DEFAULT_INIT_SIZE);
    }

    /**
     * Create an array container with specified capacity
     *
     * @param capacity The capacity of the container
     */
    public ArrayContainer(final int capacity) {
        content = new short[capacity];
    }

    /**
     * Create an array container with a run of ones from firstOfRun to
     * lastOfRun, inclusive. Caller is responsible for making sure the range
     * is small enough that ArrayContainer is appropriate.
     *
     * @param firstOfRun first index
     * @param lastOfRun  last index (range is inclusive)
     */
    public ArrayContainer(final int firstOfRun, final int lastOfRun) {
        final int valuesInRange = lastOfRun - firstOfRun + 1;
        this.content = new short[valuesInRange];
        for (int i = 0; i < valuesInRange; ++i)
            content[i] = (short) (firstOfRun + i);
        cardinality = valuesInRange;
    }

    private ArrayContainer(int newCard, short[] newContent) {
        this.cardinality = newCard;
        this.content = Arrays.copyOf(newContent, newCard);
    }

    protected ArrayContainer(short[] newContent) {
        this.cardinality = newContent.length;
        this.content = newContent;
    }

    /**
     * running time is in O(n) time if insert is not in order.
     */
    @Override
    public Container add(final short x) {
        // Transform the ArrayContainer to a BitmapContainer
        // when cardinality = DEFAULT_MAX_SIZE
        if (cardinality >= DEFAULT_MAX_SIZE) {
            BitmapContainer a = this.toBitmapContainer();
            a.add(x);
            return a;
        }
        if ((cardinality == 0) || (Util.toIntUnsigned(x) > Util.toIntUnsigned(content[cardinality - 1]))) {
            if (cardinality >= this.content.length)
                increaseCapacity();
            content[cardinality++] = x;
            return this;
        }
        int loc = Util.unsignedBinarySearch(content, 0, cardinality, x);
        if (loc < 0) {
            if (cardinality >= this.content.length)
                increaseCapacity();
            // insertion : shift the elements > x by one position to
            // the right
            // and put x in it's appropriate place
            System.arraycopy(content, -loc - 1, content, -loc,cardinality + loc + 1);
            content[-loc - 1] = x;
            ++cardinality;
        }
        return this;
    }

    @Override
    public ArrayContainer and(final ArrayContainer value2) {

        ArrayContainer value1 = this;
        final int desiredCapacity = Math.min(value1.getCardinality(), value2.getCardinality());
        ArrayContainer answer = new ArrayContainer(desiredCapacity);
        answer.cardinality = Util.unsignedIntersect2by2(value1.content,
                value1.getCardinality(), value2.content,
                value2.getCardinality(), answer.content);
        return answer;
    }

    @Override
    public Container and(BitmapContainer x) {
        return x.and(this);
    }

    @Override
    public ArrayContainer andNot(final ArrayContainer value2) {
        ArrayContainer value1 = this;
        final int desiredCapacity = value1.getCardinality();
        ArrayContainer answer = new ArrayContainer(desiredCapacity);
        answer.cardinality = Util.unsignedDifference(value1.content,
                value1.getCardinality(), value2.content,
                value2.getCardinality(), answer.content);
        return answer;
    }

    @Override
    public ArrayContainer andNot(BitmapContainer value2) {
        final ArrayContainer answer = new ArrayContainer(content.length);
        int pos = 0;
        for (int k = 0; k < cardinality; ++k)
            if (!value2.contains(this.content[k]))
                answer.content[pos++] = this.content[k];
        answer.cardinality = pos;
        return answer;
    }

    @Override
    public void clear() {
        cardinality = 0;
    }

    @Override
    public ArrayContainer clone() {
        return new ArrayContainer(this.cardinality, this.content);
    }

    @Override
    public boolean contains(final short x) {
        return Util.unsignedBinarySearch(content, 0, cardinality, x) >= 0;
    }

    @Override
    public void deserialize(DataInput in) throws IOException {
        byte[] buffer = new byte[2];
        // little endian
        in.readFully(buffer);
        this.cardinality = (buffer[0] & 0xFF) | ((buffer[1] & 0xFF) << 8);
        if (this.content.length < this.cardinality)
            this.content = new short[this.cardinality];
        for (int k = 0; k < this.cardinality; ++k) {
            in.readFully(buffer);
            this.content[k] = (short) (((buffer[1] & 0xFF) << 8) | (buffer[0] & 0xFF));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ArrayContainer) {
            ArrayContainer srb = (ArrayContainer) o;
            if (srb.cardinality != this.cardinality)
                return false;
            for (int i = 0; i < this.cardinality; ++i) {
                if (this.content[i] != srb.content[i])
                    return false;
            }
            return true;
        }
        return false;
    }

    @Override
    public void fillLeastSignificant16bits(int[] x, int i, int mask) {
        for (int k = 0; k < this.cardinality; ++k)
            x[k + i] = Util.toIntUnsigned(this.content[k]) | mask;

    }

    @Override
    protected int getArraySizeInBytes() {
        return cardinality * 2;
    }

    @Override
    public int getCardinality() {
        return cardinality;
    }

    @Override
    public ShortIterator getShortIterator() {
        return new ShortIterator() {
            int pos = 0;

            @Override
            public boolean hasNext() {
                return pos < ArrayContainer.this.cardinality;
            }

            @Override
            public short next() {
                return ArrayContainer.this.content[pos++];
            }

        };
    }

    @Override
    public int getSizeInBytes() {
        return this.cardinality * 2 + 4;

    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (int k = 0; k < cardinality; ++k)
            hash += 31 * content[k];
        return hash;
    }

    @Override
    public ArrayContainer iand(final ArrayContainer value2) {
        ArrayContainer value1 = this;
        value1.cardinality = Util.unsignedIntersect2by2(value1.content,
                value1.getCardinality(), value2.content,
                value2.getCardinality(), value1.content);
        return this;
    }

    @Override
    public Container iand(BitmapContainer value2) {
        int pos = 0;
        for (int k = 0; k < cardinality; ++k)
            if (value2.contains(this.content[k]))
                this.content[pos++] = this.content[k];
        cardinality = pos;
        return this;
    }

    @Override
    public ArrayContainer iandNot(final ArrayContainer value2) {
        this.cardinality = Util.unsignedDifference(this.content,
                this.getCardinality(), value2.content,
                value2.getCardinality(), this.content);
        return this;
    }

    @Override
    public ArrayContainer iandNot(BitmapContainer value2) {
        int pos = 0;
        for (int k = 0; k < cardinality; ++k)
            if (!value2.contains(this.content[k]))
                this.content[pos++] = this.content[k];
        this.cardinality = pos;
        return this;
    }

    private void increaseCapacity() {
        int newCapacity = this.content.length < 64 ? this.content.length * 2
                : this.content.length < 1024 ? this.content.length * 3 / 2
                : this.content.length * 5 / 4;
        if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE)
            newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
        this.content = Arrays.copyOf(this.content, newCapacity);
    }

    @Override
    public Container inot(final int firstOfRange, final int lastOfRange) {
        // determine the span of array indices to be affected
        int startIndex = Util.unsignedBinarySearch(content, 0, cardinality, (short) firstOfRange);
        if (startIndex < 0)
            startIndex = -startIndex - 1;
        int lastIndex = Util.unsignedBinarySearch(content, 0, cardinality, (short) lastOfRange);
        if (lastIndex < 0)
            lastIndex = -lastIndex - 1 - 1;
        final int currentValuesInRange = lastIndex - startIndex + 1;
        final int spanToBeFlipped = lastOfRange - firstOfRange + 1;
        final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
        final short[] buffer = new short[newValuesInRange];
        final int cardinalityChange = newValuesInRange - currentValuesInRange;
        final int newCardinality = cardinality + cardinalityChange;

        if (cardinalityChange > 0) { // expansion, right shifting needed
            if (newCardinality > content.length) {
                // so big we need a bitmap?
                if (newCardinality >= DEFAULT_MAX_SIZE)
                    return toBitmapContainer().inot(firstOfRange, lastOfRange);
                content = Arrays.copyOf(content, newCardinality);
            }
            // slide right the contentsafter the range
            System.arraycopy(content, lastIndex + 1, content,
                    lastIndex + 1 + cardinalityChange, cardinality - 1 - lastIndex);
            negateRange(buffer, startIndex, lastIndex,
                    firstOfRange, lastOfRange);
        } else { // no expansion needed
            negateRange(buffer, startIndex, lastIndex, firstOfRange, lastOfRange);
            if (cardinalityChange < 0) {
                // contraction, left sliding.
                // Leave array oversize
                System.arraycopy(content, startIndex + newValuesInRange - cardinalityChange,
                        content, startIndex + newValuesInRange,
                        newCardinality - (startIndex + newValuesInRange));
            }
        }
        cardinality = newCardinality;
        return this;
    }

    @Override
    public Container ior(final ArrayContainer value2) {
        return this.or(value2);
    }

    @Override
    public Container ior(BitmapContainer x) {
        return x.or(this);
    }

    @Override
    public Iterator<Short> iterator() {
        return new Iterator<Short>() {
            short pos = 0;

            @Override
            public boolean hasNext() {
                return pos < ArrayContainer.this.cardinality;
            }

            @Override
            public Short next() {
                return ArrayContainer.this.content[pos++];
            }

            @Override
            public void remove() {
                ArrayContainer.this.remove((short) (pos - 1));
                pos--;
            }
        };
    }

    @Override
    public Container ixor(final ArrayContainer value2) {
        return this.xor(value2);
    }

    @Override
    public Container ixor(BitmapContainer x) {
        return x.xor(this);
    }

    protected void loadData(final BitmapContainer bitmapContainer) {
        this.cardinality = bitmapContainer.cardinality;
        bitmapContainer.fillArray(content);
    }

    // for use in inot range known to be nonempty
    private void negateRange(final short[] buffer, final int startIndex,
                             final int lastIndex, final int startRange, final int lastRange) {
        // compute the negation into buffer

        int outPos = 0;
        int inPos = startIndex; // value here always >= valInRange,
        // until it is exhausted
        // n.b., we can start initially exhausted.

        int valInRange = startRange;
        for (; valInRange <= lastRange && inPos <= lastIndex; ++valInRange) {
            if ((short) valInRange != content[inPos]) {
                buffer[outPos++] = (short) valInRange;
            } else {
                ++inPos;
            }
        }

        // if there are extra items (greater than the biggest
        // pre-existing one in range), buffer them
        for (; valInRange <= lastRange; ++valInRange) {
            buffer[outPos++] = (short) valInRange;
        }

        if (outPos != buffer.length) {
            throw new RuntimeException("negateRange: outPos "
                    + outPos + " whereas buffer.length="
                    + buffer.length);
        }
        // copy back from buffer...caller must ensure there is room
        int i = startIndex;
        for (short item : buffer)
            content[i++] = item;
    }

    // shares lots of code with inot; candidate for refactoring
    @Override
    public Container not(final int firstOfRange, final int lastOfRange) {
        if (firstOfRange > lastOfRange) {
            return clone(); // empty range
        }

        // determine the span of array indices to be affected
        int startIndex = Util.unsignedBinarySearch(content, 0,
                cardinality, (short) firstOfRange);
        if (startIndex < 0)
            startIndex = -startIndex - 1;
        int lastIndex = Util.unsignedBinarySearch(content, 0,
                cardinality, (short) lastOfRange);
        if (lastIndex < 0)
            lastIndex = -lastIndex - 2;
        final int currentValuesInRange = lastIndex - startIndex + 1;
        final int spanToBeFlipped = lastOfRange - firstOfRange + 1;
        final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
        final int cardinalityChange = newValuesInRange - currentValuesInRange;
        final int newCardinality = cardinality + cardinalityChange;

        if (newCardinality >= DEFAULT_MAX_SIZE)
            return toBitmapContainer().not(firstOfRange, lastOfRange);

        ArrayContainer answer = new ArrayContainer(newCardinality);

        // copy stuff before the active area
        System.arraycopy(content, 0, answer.content, 0, startIndex);

        int outPos = startIndex;
        int inPos = startIndex; // item at inPos always >= valInRange

        int valInRange = firstOfRange;
        for (; valInRange <= lastOfRange && inPos <= lastIndex; ++valInRange) {
            if ((short) valInRange != content[inPos]) {
                answer.content[outPos++] = (short) valInRange;
            } else {
                ++inPos;
            }
        }

        for (; valInRange <= lastOfRange; ++valInRange) {
            answer.content[outPos++] = (short) valInRange;
        }

        // content after the active range
        for (int i = lastIndex + 1; i < cardinality; ++i)
            answer.content[outPos++] = content[i];
        answer.cardinality = newCardinality;
        return answer;
    }

    @Override
    public Container or(final ArrayContainer value2) {
        final ArrayContainer value1 = this;
        int totalCardinality = value1.getCardinality() + value2.getCardinality();
        if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
            BitmapContainer bc = new BitmapContainer();
            for (int k = 0; k < value2.cardinality; ++k) {
                final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
                bc.bitmap[i] |= (1l << value2.content[k]);
            }
            for (int k = 0; k < this.cardinality; ++k) {
                final int i = Util.toIntUnsigned(this.content[k]) >>> 6;
                bc.bitmap[i] |= (1l << this.content[k]);
            }
            bc.cardinality = 0;
            for (long k : bc.bitmap) {
                bc.cardinality += Long.bitCount(k);
            }
            if (bc.cardinality <= DEFAULT_MAX_SIZE)
                return bc.toArrayContainer();
            return bc;
        }
        final int desiredCapacity = totalCardinality; // Math.min(BitmapContainer.MAX_CAPACITY,
        // totalCardinality);
        ArrayContainer answer = new ArrayContainer(desiredCapacity);
        answer.cardinality = Util.unsignedUnion2by2(value1.content,
                value1.getCardinality(), value2.content,
                value2.getCardinality(), answer.content);
        return answer;
    }

    @Override
    public Container or(BitmapContainer x) {
        return x.or(this);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public Container remove(final short x) {
        final int loc = Util.unsignedBinarySearch(content, 0, cardinality, x);
        if (loc >= 0) {
            // insertion
            System.arraycopy(content, loc + 1, content, loc, cardinality - loc - 1);
            --cardinality;
        }
        return this;
    }

    @Override
    public void serialize(DataOutput out) throws IOException {
        out.write((this.cardinality) & 0xFF);
        out.write((this.cardinality >>> 8) & 0xFF);
        // little endian
        for (int k = 0; k < this.cardinality; ++k) {
            out.write((this.content[k]) & 0xFF);
            out.write((this.content[k] >>> 8) & 0xFF);
        }
    }

    @Override
    public int serializedSizeInBytes() {
        return cardinality * 2 + 2;
    }

    /**
     * Copies the data in a bitmap container.
     *
     * @return the bitmap container
     */
    public BitmapContainer toBitmapContainer() {
        BitmapContainer bc = new BitmapContainer();
        bc.loadData(this);
        return bc;
    }

    @Override
    public String toString() {
        if (this.cardinality == 0)
            return "{}";
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < this.cardinality - 1; i++) {
            sb.append(this.content[i]);
            sb.append(",");
        }
        sb.append(this.content[this.cardinality - 1]);
        sb.append("}");
        return sb.toString();
    }

    @Override
    public void trim() {
        this.content = Arrays.copyOf(this.content, this.cardinality);
    }

    @Override
    protected void writeArray(DataOutput out) throws IOException {
        // little endian
        for (int k = 0; k < this.cardinality; ++k) {
            out.write((this.content[k]) & 0xFF);
            out.write((this.content[k] >>> 8) & 0xFF);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public Container xor(final ArrayContainer value2) {
        final ArrayContainer value1 = this;
        final int totalCardinality = value1.getCardinality() + value2.getCardinality();
        if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
            BitmapContainer bc = new BitmapContainer();
            for (int k = 0; k < value2.cardinality; ++k) {
                final int i = Util.toIntUnsigned(value2.content[k]) >>> 6;
                bc.bitmap[i] ^= (1l << value2.content[k]);
            }
            for (int k = 0; k < this.cardinality; ++k) {
                final int i = Util.toIntUnsigned(this.content[k]) >>> 6;
                bc.bitmap[i] ^= (1l << this.content[k]);
            }
            bc.cardinality = 0;
            for (long k : bc.bitmap) {
                bc.cardinality += Long.bitCount(k);
            }
            if (bc.cardinality <= DEFAULT_MAX_SIZE)
                return bc.toArrayContainer();
            return bc;
        }
        final int desiredCapacity = totalCardinality;
        ArrayContainer answer = new ArrayContainer(desiredCapacity);
        answer.cardinality = Util.unsignedExclusiveUnion2by2(value1.content,
                value1.getCardinality(), value2.content,
                value2.getCardinality(), answer.content);
        return answer;
    }

    @Override
    public Container xor(BitmapContainer x) {
        return x.xor(this);
    }
}
