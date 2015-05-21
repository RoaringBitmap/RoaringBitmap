/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.ShortIterator;
import org.roaringbitmap.Util;

import java.io.*;
import java.nio.ShortBuffer;
import java.util.Iterator;

/**
 * Simple container made of an array of 16-bit integers. Unlike
 * org.roaringbitmap.ArrayContainer, this class uses a ShortBuffer to store
 * data.
 */
public final class MappeableArrayContainer extends MappeableContainer implements
        Cloneable, Serializable {
    private static final int DEFAULT_INIT_SIZE = 4;

    protected static final int DEFAULT_MAX_SIZE = 4096;

    private static final long serialVersionUID = 1L;

    protected int cardinality = 0;

    protected ShortBuffer content;

    /**
     * Create an array container with default capacity
     */
    public MappeableArrayContainer() {
        this(DEFAULT_INIT_SIZE);
    }

    /**
     * Create an array container with specified capacity
     * 
     * @param capacity
     *            The capacity of the container
     */
    public MappeableArrayContainer(final int capacity) {
        content = ShortBuffer.allocate(capacity);
    }

    /**
     * Create an array container with a run of ones from firstOfRun to
     * lastOfRun, inclusive. Caller is responsible for making sure the range is
     * small enough that ArrayContainer is appropriate.
     * 
     * @param firstOfRun
     *            first index
     * @param lastOfRun
     *            last index (range is exclusive)
     */
    public MappeableArrayContainer(final int firstOfRun, final int lastOfRun) {
        // TODO: this can be optimized for performance
        final int valuesInRange = lastOfRun - firstOfRun;
        content = ShortBuffer.allocate(valuesInRange);
        short[] sarray = content.array();
        for (int i = 0; i < valuesInRange; ++i)
            sarray[i] = (short) (firstOfRun + i);
        cardinality = valuesInRange;
    }


    
    private MappeableArrayContainer(int newCard, ShortBuffer newContent) {
        this.cardinality = newCard;
        this.content = ShortBuffer.allocate(Math.max(newCard,newContent.limit()));
        newContent.rewind();
        this.content.put(newContent);
    }

    /**
     * Construct a new ArrayContainer backed by the provided ShortBuffer. Note
     * that if you modify the ArrayContainer a new ShortBuffer may be produced.
     * 
     * @param array
     *            ShortBuffer where the data is stored
     * @param cardinality
     *            cardinality (number of values stored)
     */
    public MappeableArrayContainer(final ShortBuffer array,
            final int cardinality) {
        if (array.limit() != cardinality)
            throw new RuntimeException(
                    "Mismatch between buffer and cardinality");
        this.cardinality = cardinality;
        this.content = array;
    }

    /**
     * running time is in O(n) time if insert is not in order.
     */
    @Override
    public MappeableContainer add(final short x) {
        if (BufferUtil.isBackedBySimpleArray(this.content)) {
            short[] sarray = content.array();

            int loc = Util.unsignedBinarySearch(sarray, 0, cardinality,
                    x);
            if (loc < 0) {
                // Transform the ArrayContainer to a BitmapContainer
                // when cardinality exceeds DEFAULT_MAX_SIZE
                if (cardinality >= DEFAULT_MAX_SIZE) {
                    final MappeableBitmapContainer a = this.toBitmapContainer();
                    a.add(x);
                    return a;
                }
                if (cardinality >= sarray.length) {
                    increaseCapacity();
                    sarray = content.array();
                }
                // insertion : shift the elements > x by one
                // position to
                // the right
                // and put x in it's appropriate place
                System.arraycopy(sarray, -loc - 1, sarray, -loc, cardinality
                        + loc + 1);
                sarray[-loc - 1] = x;
                ++cardinality;
            }
        } else {

            final int loc = BufferUtil.unsignedBinarySearch(content, 0,
                    cardinality, x);
            if (loc < 0) {
                // Transform the ArrayContainer to a BitmapContainer
                // when cardinality exceeds DEFAULT_MAX_SIZE
                if (cardinality >= DEFAULT_MAX_SIZE) {
                    final MappeableBitmapContainer a = this.toBitmapContainer();
                    a.add(x);
                    return a;
                }
                if (cardinality >= this.content.limit())
                    increaseCapacity();
                // insertion : shift the elements > x by one
                // position to
                // the right
                // and put x in it's appropriate place
                for (int k = cardinality; k > -loc - 1; --k)
                    content.put(k, content.get(k - 1));
                content.put(-loc - 1, x);

                ++cardinality;
            }
        }
        return this;
    }

    @Override
    public MappeableArrayContainer and(final MappeableArrayContainer value2) {

        MappeableArrayContainer value1 = this;
        final int desiredCapacity = Math.min(value1.getCardinality(),
                value2.getCardinality());
        MappeableArrayContainer answer = new MappeableArrayContainer(
                desiredCapacity);
        if (BufferUtil.isBackedBySimpleArray(this.content) && BufferUtil.isBackedBySimpleArray(value2.content))
            answer.cardinality = org.roaringbitmap.Util.unsignedIntersect2by2(
                    value1.content.array(), value1.getCardinality(),
                    value2.content.array(), value2.getCardinality(),
                    answer.content.array());
        else
            answer.cardinality = BufferUtil.unsignedIntersect2by2(
                    value1.content, value1.getCardinality(), value2.content,
                    value2.getCardinality(), answer.content.array());
        return answer;
    }

    @Override
    public MappeableContainer and(MappeableBitmapContainer x) {
        return x.and(this);
    }

    @Override
    public MappeableArrayContainer andNot(final MappeableArrayContainer value2) {
        final MappeableArrayContainer value1 = this;
        final int desiredCapacity = value1.getCardinality();
        final MappeableArrayContainer answer = new MappeableArrayContainer(
                desiredCapacity);
        if (BufferUtil.isBackedBySimpleArray(value1.content) && BufferUtil.isBackedBySimpleArray(value2.content))
            answer.cardinality = org.roaringbitmap.Util.unsignedDifference(
                    value1.content.array(), value1.getCardinality(),
                    value2.content.array(), value2.getCardinality(),
                    answer.content.array());
        else
            answer.cardinality = BufferUtil.unsignedDifference(value1.content,
                    value1.getCardinality(), value2.content,
                    value2.getCardinality(), answer.content.array());
        return answer;
    }

    @Override
    public MappeableArrayContainer andNot(MappeableBitmapContainer value2) {

        final MappeableArrayContainer answer = new MappeableArrayContainer(
                content.limit());
        int pos = 0;
        short[] sarray = answer.content.array();
        if (BufferUtil.isBackedBySimpleArray(this.content)) {
            short[] c = content.array();
            for (int k = 0; k < cardinality; ++k)
                if (!value2.contains(c[k]))
                    sarray[pos++] = c[k];
        } else
            for (int k = 0; k < cardinality; ++k)
                if (!value2.contains(this.content.get(k)))
                    sarray[pos++] = this.content.get(k);
        answer.cardinality = pos;
        return answer;
    }

    @Override
    public void clear() {
        cardinality = 0;
    }

    @Override
    public MappeableArrayContainer clone() {
        return new MappeableArrayContainer(this.cardinality, this.content);
    }

    @Override
    public boolean contains(final short x) {
        return BufferUtil.unsignedBinarySearch(content, 0, cardinality, x) >= 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MappeableArrayContainer) {
            final MappeableArrayContainer srb = (MappeableArrayContainer) o;
            if (srb.cardinality != this.cardinality)
                return false;
            if (BufferUtil.isBackedBySimpleArray(this.content) && BufferUtil.isBackedBySimpleArray(srb.content)) {
                short[] t = this.content.array();
                short[] sr = srb.content.array();

                for (int i = 0; i < this.cardinality; ++i) {
                    if (t[i] != sr[i])
                        return false;
                }

            } else
                for (int i = 0; i < this.cardinality; ++i) {
                    if (this.content.get(i) != srb.content.get(i))
                        return false;
                }
            return true;
        }
        return false;
    }

    @Override
    public void fillLeastSignificant16bits(int[] x, int i, int mask) {
        if (BufferUtil.isBackedBySimpleArray(this.content)) {
            short[] c = this.content.array();
            for (int k = 0; k < this.cardinality; ++k)
                x[k + i] = BufferUtil.toIntUnsigned(c[k]) | mask;

        } else
            for (int k = 0; k < this.cardinality; ++k)
                x[k + i] = BufferUtil.toIntUnsigned(this.content.get(k)) | mask;
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
        return new MappeableArrayContainerShortIterator(this);
    }

    @Override
    public ShortIterator getReverseShortIterator() {
        return new ReverseMappeableArrayContainerShortIterator(this);
    }

    @Override
    public int getSizeInBytes() {
        return this.cardinality * 2;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (int k = 0; k < cardinality; ++k)
            hash += 31 * hash + content.get(k);
        return hash;
    }

    @Override
    public MappeableArrayContainer iand(final MappeableArrayContainer value2) {
        final MappeableArrayContainer value1 = this;
        value1.cardinality = BufferUtil.unsignedIntersect2by2(value1.content,
                value1.getCardinality(), value2.content,
                value2.getCardinality(), value1.content.array());
        return this;
    }

    @Override
    public MappeableContainer iand(MappeableBitmapContainer value2) {
        int pos = 0;
        for (int k = 0; k < cardinality; ++k)
            if (value2.contains(this.content.get(k)))
                this.content.put(pos++, this.content.get(k));
        cardinality = pos;
        return this;
    }

    @Override
    public MappeableArrayContainer iandNot(final MappeableArrayContainer value2) {
        if (BufferUtil.isBackedBySimpleArray(value2.content))
            this.cardinality = org.roaringbitmap.Util.unsignedDifference(
                    this.content.array(), this.getCardinality(),
                    value2.content.array(), value2.getCardinality(),
                    this.content.array());
        else
            this.cardinality = BufferUtil.unsignedDifference(this.content,
                    this.getCardinality(), value2.content,
                    value2.getCardinality(), this.content.array());

        return this;
    }

    @Override
    public MappeableArrayContainer iandNot(MappeableBitmapContainer value2) {
        short[] c = this.content.array();
        int pos = 0;
        for (int k = 0; k < cardinality; ++k)
            if (!value2.contains(c[k]))
                c[pos++] = c[k];
        this.cardinality = pos;
        return this;
    }

    private void increaseCapacity() {
        int newCapacity = (this.content.limit() == 0) ? DEFAULT_INIT_SIZE : this.content.limit() < 64 ? this.content.limit() * 2
                : this.content.limit() < 1024 ? this.content.limit() * 3 / 2
                        : this.content.limit() * 5 / 4;
        if (newCapacity > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
        final ShortBuffer newContent = ShortBuffer.allocate(newCapacity);
        this.content.rewind();
        newContent.put(this.content);
        this.content = newContent;
    }

    private void increaseCapacity(int min) {
        int newCapacity = (this.content.limit() == 0) ? DEFAULT_INIT_SIZE : this.content.limit() < 64 ? this.content.limit() * 2
                : this.content.limit() < 1024 ? this.content.limit() * 3 / 2
                        : this.content.limit() * 5 / 4;
        if(newCapacity < min) newCapacity = min;
        if (newCapacity > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
        final ShortBuffer newContent = ShortBuffer.allocate(newCapacity);
        this.content.rewind();
        newContent.put(this.content);
        this.content = newContent;
    }

    @Override
    public MappeableContainer inot(final int firstOfRange, final int lastOfRange) {
        // TODO: this can be optimized for performance
        // determine the span of array indices to be affected
        int startIndex = BufferUtil.unsignedBinarySearch(content, 0,
                cardinality, (short) firstOfRange);
        if (startIndex < 0)
            startIndex = -startIndex - 1;
        int lastIndex = BufferUtil.unsignedBinarySearch(content, 0,
                cardinality, (short) (lastOfRange - 1));
        if (lastIndex < 0)
            lastIndex = -lastIndex - 1 - 1;
        final int currentValuesInRange = lastIndex - startIndex + 1;
        final int spanToBeFlipped = lastOfRange - firstOfRange ;
        final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
        final ShortBuffer buffer = ShortBuffer.allocate(newValuesInRange);
        final int cardinalityChange = newValuesInRange - currentValuesInRange;
        final int newCardinality = cardinality + cardinalityChange;

        if (cardinalityChange > 0) { // expansion, right shifting needed
            if (newCardinality > content.limit()) {
                // so big we need a bitmap?
                if (newCardinality >= DEFAULT_MAX_SIZE)
                    return toBitmapContainer().inot(firstOfRange, lastOfRange);
                final ShortBuffer co = ShortBuffer.allocate(newCardinality);
                content.rewind();
                co.put(content);
                content = co;
            }
            // slide right the contents after the range
            for (int pos = cardinality - 1; pos > lastIndex; --pos)
                content.put(pos + cardinalityChange, content.get(pos));
            negateRange(buffer, startIndex, lastIndex, firstOfRange,
                    lastOfRange);
        } else { // no expansion needed
            negateRange(buffer, startIndex, lastIndex, firstOfRange,
                    lastOfRange);
            if (cardinalityChange < 0) // contraction, left sliding.
                // Leave array oversize
                for (int i = startIndex + newValuesInRange; i < newCardinality; ++i)
                    content.put(i, content.get(i - cardinalityChange));
        }
        cardinality = newCardinality;
        return this;
    }

    @Override
    public MappeableContainer ior(final MappeableArrayContainer value2) {
        return this.or(value2);
    }

    @Override
    public MappeableContainer ior(MappeableBitmapContainer x) {
        return x.or(this);
    }

    @Override
    public Iterator<Short> iterator() {

        return new Iterator<Short>() {
            short pos = 0;

            @Override
            public boolean hasNext() {
                return pos < MappeableArrayContainer.this.cardinality;
            }

            @Override
            public Short next() {
                return MappeableArrayContainer.this.content.get(pos++);
            }

            @Override
            public void remove() {
                MappeableArrayContainer.this.remove((short) (pos - 1));
                pos--;
            }
        };
    }

    @Override
    public MappeableContainer ixor(final MappeableArrayContainer value2) {
        return this.xor(value2);
    }

    @Override
    public MappeableContainer ixor(MappeableBitmapContainer x) {
        return x.xor(this);
    }

    protected void loadData(final MappeableBitmapContainer bitmapContainer) {
        this.cardinality = bitmapContainer.cardinality;
        bitmapContainer.fillArray(content.array());
    }

    // for use in inot range known to be nonempty
    private void negateRange(final ShortBuffer buffer, final int startIndex,
            final int lastIndex, final int startRange, final int lastRange) {
        // compute the negation into buffer

        int outPos = 0;
        int inPos = startIndex; // value here always >= valInRange,
        // until it is exhausted
        // n.b., we can start initially exhausted.

        int valInRange = startRange;
        for (; valInRange < lastRange && inPos <= lastIndex; ++valInRange) {
            if ((short) valInRange != content.get(inPos))
                buffer.put(outPos++, (short) valInRange);
            else {
                ++inPos;
            }
        }

        // if there are extra items (greater than the biggest
        // pre-existing one in range), buffer them
        for (; valInRange < lastRange; ++valInRange) {
            buffer.put(outPos++, (short) valInRange);
        }

        if (outPos != buffer.limit()) {
            throw new RuntimeException("negateRange: outPos " + outPos
                    + " whereas buffer.length=" + buffer.limit());
        }
        assert outPos == buffer.limit();
        // copy back from buffer...caller must ensure there is room
        int i = startIndex;
        for (int k = 0; k < buffer.limit(); ++k) {
            final short item = buffer.get(k);
            content.put(i++, item);
        }
    }

    // shares lots of code with inot; candidate for refactoring
    @Override
    public MappeableContainer not(final int firstOfRange, final int lastOfRange) {
        // TODO: this can be optimized for performance
        if (firstOfRange >= lastOfRange) {
            return clone(); // empty range
        }

        // determine the span of array indices to be affected
        int startIndex = BufferUtil.unsignedBinarySearch(content, 0,
                cardinality, (short) firstOfRange);
        if (startIndex < 0)
            startIndex = -startIndex - 1;
        int lastIndex = BufferUtil.unsignedBinarySearch(content, 0,
                cardinality, (short) (lastOfRange-1));
        if (lastIndex < 0)
            lastIndex = -lastIndex - 2;
        final int currentValuesInRange = lastIndex - startIndex + 1;
        final int spanToBeFlipped = lastOfRange - firstOfRange ;
        final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
        final int cardinalityChange = newValuesInRange - currentValuesInRange;
        final int newCardinality = cardinality + cardinalityChange;

        if (newCardinality >= DEFAULT_MAX_SIZE)
            return toBitmapContainer().not(firstOfRange, lastOfRange);

        final MappeableArrayContainer answer = new MappeableArrayContainer(
                newCardinality);
        short[] sarray = answer.content.array();

        for (int i = 0; i < startIndex; ++i)
            // copy stuff before the active area
            sarray[i] = content.get(i);

        int outPos = startIndex;
        int inPos = startIndex; // item at inPos always >= valInRange

        int valInRange = firstOfRange;
        for (; valInRange < lastOfRange && inPos <= lastIndex; ++valInRange) {
            if ((short) valInRange != content.get(inPos))
                sarray[outPos++] = (short) valInRange;
            else {
                ++inPos;
            }
        }

        for (; valInRange < lastOfRange; ++valInRange) {
            answer.content.put(outPos++, (short) valInRange);
        }

        // content after the active range
        for (int i = lastIndex + 1; i < cardinality; ++i)
            answer.content.put(outPos++, content.get(i));
        answer.cardinality = newCardinality;
        return answer;
    }

    @Override
    public MappeableContainer or(final MappeableArrayContainer value2) {
        final MappeableArrayContainer value1 = this;
        final int totalCardinality = value1.getCardinality()
                + value2.getCardinality();
        if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
            final MappeableBitmapContainer bc = new MappeableBitmapContainer();
            long[] bitArray = bc.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(value2.content)) {
                short[] sarray = value2.content.array();
                for (int k = 0; k < value2.cardinality; ++k) {
                    final int i = BufferUtil.toIntUnsigned(sarray[k]) >>> 6;
                    bitArray[i] |= (1l << sarray[k]);
                }
            } else
                for (int k = 0; k < value2.cardinality; ++k) {
                    final int i = BufferUtil.toIntUnsigned(value2.content
                            .get(k)) >>> 6;
                    bitArray[i] |= (1l << value2.content.get(k));
                }
            if (BufferUtil.isBackedBySimpleArray(this.content)) {
                short[] sarray = this.content.array();
                for (int k = 0; k < this.cardinality; ++k) {
                    final int i = BufferUtil.toIntUnsigned(sarray[k]) >>> 6;
                    bitArray[i] |= (1l << sarray[k]);
                }
            } else
                for (int k = 0; k < this.cardinality; ++k) {
                    final int i = BufferUtil.toIntUnsigned(this.content.get(k)) >>> 6;
                    bitArray[i] |= (1l << this.content.get(k));
                }
            bc.cardinality = 0;
            for (int index = 0; index < bc.bitmap.limit(); ++index) {
                bc.cardinality += Long.bitCount(bitArray[index]);
            }
            if (bc.cardinality <= DEFAULT_MAX_SIZE)
                return bc.toArrayContainer();
            return bc;
        }
        final MappeableArrayContainer answer = new MappeableArrayContainer(
                totalCardinality);
        if (BufferUtil.isBackedBySimpleArray(value1.content) && BufferUtil.isBackedBySimpleArray(value2.content))
            answer.cardinality = org.roaringbitmap.Util.unsignedUnion2by2(
                    value1.content.array(), value1.getCardinality(),
                    value2.content.array(), value2.getCardinality(),
                    answer.content.array());

        else
            answer.cardinality = BufferUtil.unsignedUnion2by2(value1.content,
                    value1.getCardinality(), value2.content,
                    value2.getCardinality(), answer.content.array());
        return answer;
    }

    @Override
    public MappeableContainer or(MappeableBitmapContainer x) {
        return x.or(this);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        // little endian
        this.cardinality = 0xFFFF & Short.reverseBytes(in.readShort());
        if (this.content.limit() < this.cardinality)
            this.content = ShortBuffer.allocate(this.cardinality);
        for (int k = 0; k < this.cardinality; ++k) {
            this.content.put(k,Short.reverseBytes(in.readShort()));
        }
    }

    @Override
	public MappeableContainer remove(final short x) {
		if (BufferUtil.isBackedBySimpleArray(this.content)) {
			final int loc = Util.unsignedBinarySearch(content.array(), 0,
					cardinality, x);
			if (loc >= 0) {
				// insertion
				System.arraycopy(content.array(), loc + 1, content.array(), loc,
						cardinality - loc - 1);
				--cardinality;
			}
			return this;
		} else {
			final int loc = BufferUtil.unsignedBinarySearch(content, 0, cardinality,
					x);
			if (loc >= 0) {
				// insertion
				for (int k = loc + 1; k < cardinality; --k) {
					content.put(k - 1, content.get(k));
				}
				--cardinality;
			}
			return this;

		}
	}

    /**
     * Copies the data in a bitmap container.
     * 
     * @return the bitmap container
     */
    public MappeableBitmapContainer toBitmapContainer() {
        final MappeableBitmapContainer bc = new MappeableBitmapContainer();
        bc.loadData(this);
        return bc;
    }

    @Override
    public String toString() {
        if (this.cardinality == 0)
            return "{}";
        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < this.cardinality - 1; i++) {
            sb.append(this.content.get(i));
            sb.append(",");
        }
        sb.append(this.content.get(this.cardinality - 1));
        sb.append("}");
        return sb.toString();
    }

    @Override
    public void trim() {
        final ShortBuffer co = ShortBuffer.allocate(this.cardinality);
        for (int k = 0; k < this.cardinality; ++k)
            co.put(this.content.get(k));
        this.content = co;
    }

    @Override
    protected void writeArray(DataOutput out) throws IOException {
        // little endian
        for (int k = 0; k < this.cardinality; ++k) {
            out.write(this.content.get(k) & 0xFF);
            out.write((this.content.get(k) >>> 8) & 0xFF);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.write(this.cardinality & 0xFF);
        out.write((this.cardinality >>> 8) & 0xFF);
        // little endian
        for (int k = 0; k < this.cardinality; ++k) {
            out.write(this.content.get(k) & 0xFF);
            out.write((this.content.get(k) >>> 8) & 0xFF);
        }
    }

    @Override
    public MappeableContainer xor(final MappeableArrayContainer value2) {
        final MappeableArrayContainer value1 = this;
        final int totalCardinality = value1.getCardinality()
                + value2.getCardinality();
        if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
            final MappeableBitmapContainer bc = new MappeableBitmapContainer();
            long[] bitArray = bc.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(value2.content)) {
                short[] sarray = value2.content.array();
                for (int k = 0; k < value2.cardinality; ++k) {
                    final int i = BufferUtil.toIntUnsigned(sarray[k]) >>> 6;
                    bitArray[i] ^= (1l << sarray[k]);
                }
            } else
                for (int k = 0; k < value2.cardinality; ++k) {
                    final int i = BufferUtil.toIntUnsigned(value2.content
                            .get(k)) >>> 6;
                    bitArray[i] ^= (1l << value2.content.get(k));
                }
            if (BufferUtil.isBackedBySimpleArray(this.content)) {
                short[] sarray = this.content.array();
                for (int k = 0; k < this.cardinality; ++k) {
                    final int i = BufferUtil.toIntUnsigned(sarray[k]) >>> 6;
                    bitArray[i] ^= (1l << sarray[k]);
                }
            } else
                for (int k = 0; k < this.cardinality; ++k) {
                    final int i = BufferUtil.toIntUnsigned(this.content.get(k)) >>> 6;
                    bitArray[i] ^= (1l << this.content.get(k));
                }

            bc.cardinality = 0;
            for (int index = 0; index < bc.bitmap.limit(); ++index) {
                bc.cardinality += Long.bitCount(bitArray[index]);
            }
            if (bc.cardinality <= DEFAULT_MAX_SIZE)
                return bc.toArrayContainer();
            return bc;
        }
        final MappeableArrayContainer answer = new MappeableArrayContainer(
                totalCardinality);
        if (BufferUtil.isBackedBySimpleArray(value1.content) && BufferUtil.isBackedBySimpleArray(value2.content))
            answer.cardinality = org.roaringbitmap.Util
                    .unsignedExclusiveUnion2by2(value1.content.array(),
                            value1.getCardinality(), value2.content.array(),
                            value2.getCardinality(), answer.content.array());
        else
            answer.cardinality = BufferUtil.unsignedExclusiveUnion2by2(
                    value1.content, value1.getCardinality(), value2.content,
                    value2.getCardinality(), answer.content.array());
        return answer;
    }

    @Override
    public MappeableContainer xor(MappeableBitmapContainer x) {
        return x.xor(this);
    }

    @Override
    public int rank(short lowbits) {
        int answer =  BufferUtil.unsignedBinarySearch(content, 0, cardinality, lowbits);
        if (answer >= 0) {
            return answer + 1;
        } else {
            return -answer - 1;
        }
    }
    

    @Override
    public short select(int j) {
        return this.content.get(j);
    }

    @Override
    public MappeableContainer limit(int maxcardinality) {
        if (maxcardinality < this.getCardinality())
            return new MappeableArrayContainer(maxcardinality, this.content);
        else
            return clone();
    }

		@Override
		public MappeableContainer flip(short x) {
      if (BufferUtil.isBackedBySimpleArray(this.content)) {
        short[] sarray = content.array();
				int loc = Util.unsignedBinarySearch(sarray, 0, cardinality, x);
	      if (loc < 0) {
	          // Transform the ArrayContainer to a BitmapContainer
	          // when cardinality = DEFAULT_MAX_SIZE
	          if (cardinality >= DEFAULT_MAX_SIZE) {
	              MappeableBitmapContainer a = this.toBitmapContainer();
	              a.add(x);
	              return a;
	          }
	          if (cardinality >= sarray.length) {
	              increaseCapacity();
	              sarray = content.array();
	          }
	          // insertion : shift the elements > x by one position to
	          // the right
	          // and put x in it's appropriate place
	          System.arraycopy(sarray, -loc - 1, sarray, -loc,cardinality + loc + 1);
	          sarray[-loc - 1] = x;
	          ++cardinality;
	      } else {
	        System.arraycopy(sarray, loc + 1, sarray, loc, cardinality - loc - 1);
	        --cardinality;
	      }
	      return this;

			} else {
				int loc = BufferUtil.unsignedBinarySearch(content, 0, cardinality, x);
	      if (loc < 0) {
	          // Transform the ArrayContainer to a BitmapContainer
	          // when cardinality = DEFAULT_MAX_SIZE
	          if (cardinality >= DEFAULT_MAX_SIZE) {
	              MappeableBitmapContainer a = this.toBitmapContainer();
	              a.add(x);
	              return a;
	          }
	          if (cardinality >= content.limit()) {
	              increaseCapacity();
	          }
	          // insertion : shift the elements > x by one position to
	          // the right
	          // and put x in it's appropriate place
	          for (int k = cardinality; k > -loc - 1; --k)
	            content.put(k, content.get(k - 1));
	          content.put(-loc - 1, x);
	          ++cardinality;
	      } else {
					for (int k = loc + 1; k < cardinality; --k) {
						content.put(k - 1, content.get(k));
					}
					--cardinality;
	      }
	      return this;
			}
		}

	    @Override
	    public MappeableContainer add(int begin, int end) {

	        int indexstart = BufferUtil.unsignedBinarySearch(content, 0, cardinality,
	                (short) begin);
	        if (indexstart < 0)
	            indexstart = -indexstart - 1;
	        int indexend = BufferUtil.unsignedBinarySearch(content, 0, cardinality,
	                (short) (end - 1));
	        if (indexend < 0)
	            indexend = -indexend - 1;
	        else
	            indexend++;
	        int rangelength = end - begin;
	        int newcardinality = indexstart + (cardinality - indexend)
	                + rangelength;
	        if (newcardinality >= DEFAULT_MAX_SIZE) {
	            MappeableBitmapContainer a = this.toBitmapContainer();
	            return a.iadd(begin, end);
	        }
	        MappeableArrayContainer answer = new MappeableArrayContainer(newcardinality, content);
	        BufferUtil.arraycopy(content, indexend, answer.content, indexstart
	                + rangelength, cardinality - indexend);
	        short[] answerarray = answer.content.array();
	        for (int k = 0; k < rangelength; ++k) {
	            answerarray[k + indexstart] = (short) (begin + k);
	        }
	        answer.cardinality = newcardinality;
	        return answer;
	    }

	    @Override
	    public MappeableContainer remove(int begin, int end) {
	        int indexstart = BufferUtil.unsignedBinarySearch(content, 0, cardinality,
	                (short) begin);
	        if (indexstart < 0)
	            indexstart = -indexstart - 1;
	        int indexend = BufferUtil.unsignedBinarySearch(content, 0, cardinality,
	                (short) (end - 1));
	        if (indexend < 0)
	            indexend = -indexend - 1;
	        else
	            indexend++;
	        int rangelength = indexend - indexstart;
	        MappeableArrayContainer answer = clone();
	        BufferUtil.arraycopy(content, indexstart + rangelength, answer.content,
	                indexstart, cardinality - indexstart - rangelength);
	        answer.cardinality = cardinality - rangelength;
	        return answer;
	    }

	    @Override
	    public MappeableContainer iadd(int begin, int end) {
	        int indexstart = BufferUtil.unsignedBinarySearch(content, 0, cardinality,
	                (short) begin);
	        if (indexstart < 0)
	            indexstart = -indexstart - 1;
	        int indexend = BufferUtil.unsignedBinarySearch(content, 0, cardinality,
	                (short) (end - 1));
	        if (indexend < 0)
	            indexend = -indexend - 1;
	        else indexend++;
	        int rangelength = end - begin;
	        int newcardinality = indexstart + (cardinality - indexend) + rangelength;
	        if (newcardinality >= DEFAULT_MAX_SIZE) {
	            MappeableBitmapContainer a = this.toBitmapContainer();
	            return a.iadd(begin, end);
	        }
	        if (newcardinality >= this.content.limit())
	            increaseCapacity(newcardinality);
	        BufferUtil.arraycopy(content, indexend, content, indexstart + rangelength,
	                cardinality - indexend);
            if (BufferUtil.isBackedBySimpleArray(content)) {
               short[] contentarray = content.array();
               for (int k = 0; k < rangelength; ++k) {
                  contentarray[k + indexstart] = (short) (begin + k);
               }
            } else {
                for (int k = 0; k < rangelength; ++k) {
                   content.put(k + indexstart, (short) (begin + k));   
                }
            }
	        cardinality = newcardinality;
	        return this;
	    }

	    @Override
	    public MappeableContainer iremove(int begin, int end) {
	        int indexstart = BufferUtil.unsignedBinarySearch(content, 0, cardinality,
	                (short) begin);
	        if (indexstart < 0)
	            indexstart = -indexstart - 1;
	        int indexend = BufferUtil.unsignedBinarySearch(content, 0, cardinality,
	                (short) (end - 1));
	        if (indexend < 0)
	            indexend = -indexend - 1;
	        else
	            indexend++;
	        int rangelength = indexend - indexstart;
	        BufferUtil.arraycopy(content, indexstart + rangelength, content, indexstart,
	                cardinality - indexstart - rangelength);
	        cardinality -= rangelength;
	        return this;
	    }



}



final class MappeableArrayContainerShortIterator implements ShortIterator {
    int pos;
    MappeableArrayContainer parent;

    MappeableArrayContainerShortIterator() {
    }

    
    MappeableArrayContainerShortIterator(MappeableArrayContainer p) {
        wrap(p);
    }
    
    void wrap(MappeableArrayContainer p) {
        parent = p;
        pos = 0;
    }

    @Override
    public boolean hasNext() {
        return pos < parent.cardinality;
    }

    @Override
    public short next() {
        return parent.content.get(pos++);
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
        parent.remove((short) (pos - 1));
        pos--;                
    }

}


final class ReverseMappeableArrayContainerShortIterator implements ShortIterator {
    
    int pos;
    
    MappeableArrayContainer parent;

    ReverseMappeableArrayContainerShortIterator() {
    }

    
    ReverseMappeableArrayContainerShortIterator(MappeableArrayContainer p) {
        wrap(p);
    }
    
    void wrap(MappeableArrayContainer p) {
        parent = p;
        pos = parent.cardinality - 1;
    }
    
    @Override
    public boolean hasNext() {
        return pos >= 0;
    }

    @Override
    public short next() {
        return parent.content.get(pos--);
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
        parent.remove((short) (pos + 1));
        pos++;
    }

}

