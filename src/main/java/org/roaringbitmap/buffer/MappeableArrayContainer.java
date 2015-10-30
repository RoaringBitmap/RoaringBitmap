/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.PeekableShortIterator;
import org.roaringbitmap.ShortIterator;
import org.roaringbitmap.Util;

import java.io.*;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Simple container made of an array of 16-bit integers. Unlike
 * org.roaringbitmap.ArrayContainer, this class uses a ShortBuffer to store
 * data.
 */
public final class MappeableArrayContainer extends MappeableContainer implements
        Cloneable {
    private static final int DEFAULT_INIT_SIZE = 4;

    protected static final int DEFAULT_MAX_SIZE = 4096; // containers with DEFAULT_MAX_SZE or less integers should be ArrayContainers

    protected int cardinality = 0;
    
    private static final long serialVersionUID = 1L;

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


    private  MappeableArrayContainer(int newCard, ShortBuffer newContent) {
        this.cardinality = newCard;
        ShortBuffer tmp = newContent.duplicate();// for thread-safety
        this.content = ShortBuffer.allocate(Math.max(newCard,tmp.limit()));
        tmp.rewind();
        this.content.put(tmp);
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

    @Override
    int numberOfRuns() {
        if(cardinality == 0)
            return 0; // should never happen
        
        if(BufferUtil.isBackedBySimpleArray(content)) {
            short[] c = content.array();
            int numRuns = 1;
            int oldv = BufferUtil.toIntUnsigned(c[0]);
            for (int i = 1; i < cardinality; i++) {
                int newv = BufferUtil.toIntUnsigned(c[i]);
                if(oldv + 1 != newv) ++numRuns;
                oldv = newv;
            }
            return numRuns;
        } else {
            int numRuns = 0;
            int previous = BufferUtil.toIntUnsigned(content.get(0));
            // we do not proceed like above for fear that calling "get" twice per loop would be too much 
            for (int i = 1; i < cardinality; i++) {
                int val = BufferUtil.toIntUnsigned(content.get(i));
                if (val != previous+1)
                    ++numRuns;
                previous = val;
            }
            return numRuns;
        }
    }

    /**
     * running time is in O(n) time if insert is not in order.
     */
    @Override
    // not thread-safe
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
    public MappeableContainer and(final MappeableRunContainer value2) {
        return value2.and(this);
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
            for (int k = 0; k < cardinality; ++k) {
                short v = c[k];
                if (!value2.contains(v))
                    sarray[pos++] = v;
            }
        } else
            for (int k = 0; k < cardinality; ++k) {
                short v = this.content.get(k);
                if (!value2.contains(v))
                    sarray[pos++] = v;
            }
        answer.cardinality = pos;
        return answer;
    }


    @Override
    public MappeableContainer andNot(final MappeableRunContainer x) {
        int writeLocation=0;
        int runStart, runEnd;  // the current or upcoming run.
        int whichRun;
        if (x.nbrruns == 0) return clone();

        ShortBuffer buffer = ShortBuffer.allocate(cardinality);

        runStart = BufferUtil.toIntUnsigned(x.getValue(0));
        runEnd = runStart + BufferUtil.toIntUnsigned(x.getLength(0));
        whichRun=0;

        short val;
        for (int i = 0; i < cardinality; ++i) {
            val = content.get(i);
            int valInt = BufferUtil.toIntUnsigned(val);
            if ( valInt < runStart) {
                buffer.put(writeLocation++, val);
            }
            else if (valInt <= runEnd)
                ; // don't want item
            else {
                // greater than this run, need to do an advanceUntil on runs
                // done sequentially for now (no galloping attempts).
                do {
                    if (whichRun+1 < x.nbrruns) {
                        whichRun++;
                        runStart = BufferUtil.toIntUnsigned(x.getValue(whichRun));
                        runEnd = runStart + BufferUtil.toIntUnsigned(x.getLength(whichRun));
                    }
                    else runStart = runEnd = (1 << 16) + 1;  // infinity....
                } while ( valInt > runEnd);
                --i;  // need to re-process this val
            }
        }
        return new MappeableArrayContainer(writeLocation,buffer);
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
        } else if (o instanceof MappeableRunContainer)
            return o.equals(this);
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
        return getArraySizeInBytes(cardinality);
    }

    protected static int getArraySizeInBytes(int cardinality) {
        return cardinality * 2;
    }


    @Override
    public int getCardinality() {
        return cardinality;
    }

    @Override
    public ShortIterator getShortIterator() {
        if(this.isArrayBacked())
            return new RawArrayContainerShortIterator(this);
        return new MappeableArrayContainerShortIterator(this);
    }

    @Override
    public ShortIterator getReverseShortIterator() {
        if(this.isArrayBacked())
            return new RawReverseArrayContainerShortIterator(this);
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
        if(!BufferUtil.isBackedBySimpleArray(value1.content))
            throw new RuntimeException("Should not happen. Internal bug.");
        value1.cardinality = BufferUtil.unsignedIntersect2by2(value1.content,
                value1.getCardinality(), value2.content,
                value2.getCardinality(), value1.content.array());
        return this;
    }

    @Override
    public MappeableContainer iand(MappeableBitmapContainer value2) {
        int pos = 0;
        for (int k = 0; k < cardinality; ++k) {
            short v = this.content.get(k);
            if (value2.contains(v))
                this.content.put(pos++, v);
        }
        cardinality = pos;
        return this;
    }

    // Note it is never inplace, may wish to fix
    @Override
    public MappeableContainer iand(final MappeableRunContainer value2) {
        return value2.and(this);
    }

    @Override
    public MappeableArrayContainer iandNot(final MappeableArrayContainer value2) {
        if(!BufferUtil.isBackedBySimpleArray(this.content))
            throw new RuntimeException("Should not happen. Internal bug.");
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
        if(!BufferUtil.isBackedBySimpleArray(this.content))
            throw new RuntimeException("Should not happen. Internal bug.");
        short[] c = this.content.array();
        int pos = 0;
        for (int k = 0; k < cardinality; ++k) {
            short v = c[k];
            if (!value2.contains(v))
                c[pos++] = v;
        }
        this.cardinality = pos;
        return this;
    }


    @Override
    public MappeableContainer iandNot(final MappeableRunContainer value2) { // not inplace, revisit?
        return andNot(value2);
    }


    private void increaseCapacity() { 
        increaseCapacity(false);
    }

    // temporarily allow an illegally large size, as long as the operation creating
    // the illegal container does not return it.
    // not thread safe!
    private void increaseCapacity(boolean allowIllegalSize) {
        int len = this.content.limit();
        int newCapacity = (len == 0) ? DEFAULT_INIT_SIZE : len < 64 ? len * 2
                : this.content.limit() < 1067 ? len * 3 / 2
                        : len * 5 / 4;
        // do not allocate more than we will ever need
        if (newCapacity > MappeableArrayContainer.DEFAULT_MAX_SIZE && !allowIllegalSize )
            newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
        // if we are within 1/16th of the max., go to max right away to avoid further reallocations
        if(newCapacity < MappeableArrayContainer.DEFAULT_MAX_SIZE  -  MappeableArrayContainer.DEFAULT_MAX_SIZE / 16)
            newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
        final ShortBuffer newContent = ShortBuffer.allocate(newCapacity);
        this.content.rewind();
        newContent.put(this.content);
        this.content = newContent;
    }

    // not thread safe!
    private void increaseCapacity(int min) {
        int len = this.content.limit();
        int newCapacity = (len == 0) ? DEFAULT_INIT_SIZE : len < 64 ? len * 2
                : len < 1024 ? len * 3 / 2
                        : len * 5 / 4;
        if(newCapacity < min) newCapacity = min;
        if (newCapacity > MappeableArrayContainer.DEFAULT_MAX_SIZE)
            newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
        if(newCapacity < MappeableArrayContainer.DEFAULT_MAX_SIZE  -  MappeableArrayContainer.DEFAULT_MAX_SIZE / 16)
            newCapacity = MappeableArrayContainer.DEFAULT_MAX_SIZE;
        final ShortBuffer newContent = ShortBuffer.allocate(newCapacity);
        this.content.rewind();
        newContent.put(this.content);
        this.content = newContent;
    }

    @Override
    // not thread safe! (duh!)
    public MappeableContainer inot(final int firstOfRange, final int lastOfRange) {
        // TODO: may need to convert to a RunContainer
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
                if (newCardinality > DEFAULT_MAX_SIZE)
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
    public MappeableContainer ior(final MappeableRunContainer value2) {
  // not inplace
        return value2.or(this);
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

    @Override
    public MappeableContainer ixor(final MappeableRunContainer value2) {
        return value2.xor(this);
    }

    protected void loadData(final MappeableBitmapContainer bitmapContainer) {
        this.cardinality = bitmapContainer.cardinality;
        if(!BufferUtil.isBackedBySimpleArray(this.content))
            throw new RuntimeException("Should not happen. Internal bug.");
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
        int len = buffer.limit();
        for (int k = 0; k < len; ++k) {
            final short item = buffer.get(k);
            content.put(i++, item);
        }
    }

    // shares lots of code with inot; candidate for refactoring
    @Override
    public MappeableContainer not(final int firstOfRange, final int lastOfRange) {
        // TODO: may need to convert to a RunContainer
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

        if (newCardinality > DEFAULT_MAX_SIZE)
            return toBitmapContainer().not(firstOfRange, lastOfRange);

        final MappeableArrayContainer answer = new MappeableArrayContainer(
                newCardinality);
        if(!BufferUtil.isBackedBySimpleArray(answer.content))
            throw new RuntimeException("Should not happen. Internal bug.");
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
            if(!BufferUtil.isBackedBySimpleArray(bc.bitmap))
                throw new RuntimeException("Should not happen. Internal bug.");
            long[] bitArray = bc.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(value2.content)) {
                short[] sarray = value2.content.array();
                for (int k = 0; k < value2.cardinality; ++k) {
                    short v = sarray[k];
                    final int i = BufferUtil.toIntUnsigned(v) >>> 6;
                    bitArray[i] |= (1l << v);
                }
            } else
                for (int k = 0; k < value2.cardinality; ++k) {
                    short v2 = value2.content.get(k);
                    final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
                    bitArray[i] |= (1l << v2);
                }
            if (BufferUtil.isBackedBySimpleArray(this.content)) {
                short[] sarray = this.content.array();
                for (int k = 0; k < this.cardinality; ++k) {
                    short v = sarray[k];
                    final int i = BufferUtil.toIntUnsigned(v) >>> 6;
                    bitArray[i] |= (1l << v);
                }
            } else
                for (int k = 0; k < this.cardinality; ++k) {
                    short v = this.content.get(k);
                    final int i = BufferUtil.toIntUnsigned(v) >>> 6;
                    bitArray[i] |= (1l << v);
                }
            bc.cardinality = 0;
            int len = bc.bitmap.limit();
            for (int index = 0; index < len; ++index) {
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

   
    private int advance(ShortIterator it) {
        if (it.hasNext()) 
           return  BufferUtil.toIntUnsigned(it.next());
        else
            return -1;
    }

    // in order 
    // not thread-safe
    private void emit(short val) {
        if (cardinality == content.limit())
            increaseCapacity(true);
        content.put(cardinality++, val);
    }


    /** it must return items in (unsigned) sorted order.  Possible candidate for
        Container interface?  **/
    private MappeableContainer or(ShortIterator it, boolean exclusive) {
        MappeableArrayContainer ac = new MappeableArrayContainer();
        int myItPos = 0;
        ac.cardinality=0;
        // do a merge.  int -1 denotes end of input.
        int myHead = (myItPos == cardinality) ? -1 : BufferUtil.toIntUnsigned(content.get(myItPos++));
        int hisHead =  advance(it);

        while (myHead != -1 && hisHead != -1) {
            if (myHead < hisHead) {
                ac.emit( (short) myHead);
                myHead = (myItPos == cardinality) ? -1 : BufferUtil.toIntUnsigned(content.get(myItPos++));
            }
            else if (myHead > hisHead) {
                ac.emit(  (short) hisHead);
                hisHead = advance(it);
            }
            else {
                if (! exclusive) 
                    ac.emit( (short) hisHead);
                hisHead = advance(it);
                myHead = (myItPos == cardinality) ? -1 : BufferUtil.toIntUnsigned(content.get(myItPos++));
            }
        }

        while (myHead != -1) {
            ac.emit( (short) myHead);
            myHead = (myItPos == cardinality) ? -1 : BufferUtil.toIntUnsigned(content.get(myItPos++));
        }

        while (hisHead != -1) {
            ac.emit( (short) hisHead);
            hisHead = advance(it);
        }

        if (ac.cardinality > DEFAULT_MAX_SIZE)
            return ac.toBitmapContainer();
        else return ac;
    }

    protected MappeableContainer or(ShortIterator it) {
        return or(it, false);
    }

    protected MappeableContainer xor(ShortIterator it) {
        return or(it,true);
    }


    @Override
    public MappeableContainer or(final MappeableRunContainer value2) {
        return value2.or(this);
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
    
    @Override
    public int serializedSizeInBytes() {
        return serializedSizeInBytes(cardinality);
    }


    protected static int serializedSizeInBytes( int cardinality) {
        return cardinality * 2 + 2;
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
        if(this.content.limit() == this.cardinality) return;
        if (BufferUtil.isBackedBySimpleArray(content)) {
            this.content = ShortBuffer.wrap(Arrays.copyOf(content.array(),
                    cardinality));
        } else {
            final ShortBuffer co = ShortBuffer.allocate(this.cardinality);
            // can assume that new co is array backed
            short[] x = co.array();
            for (int k = 0; k < this.cardinality; ++k)
                x[k] = this.content.get(k);
            this.content = co;
        }
    }

    @Override
    protected void writeArray(DataOutput out) throws IOException {
        // little endian
        if(BufferUtil.isBackedBySimpleArray(content)) {
            short[] a = content.array();
            for (int k = 0; k < this.cardinality; ++k) {
                out.writeShort(Short.reverseBytes(a[k]));
            }    
        } else {
            for (int k = 0; k < this.cardinality; ++k) {
                out.writeShort(Short.reverseBytes(content.get(k)));
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.write(this.cardinality & 0xFF);
        out.write((this.cardinality >>> 8) & 0xFF);
        if(BufferUtil.isBackedBySimpleArray(content)) {
            short[] a = content.array();
            for (int k = 0; k < this.cardinality; ++k) {
                out.writeShort(Short.reverseBytes(a[k]));
            }    
        } else {
            for (int k = 0; k < this.cardinality; ++k) {
                out.writeShort(Short.reverseBytes(content.get(k)));
            }
        }
    }

    @Override
    public MappeableContainer xor(final MappeableArrayContainer value2) {
        final MappeableArrayContainer value1 = this;
        final int totalCardinality = value1.getCardinality()
                + value2.getCardinality();
        if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
            final MappeableBitmapContainer bc = new MappeableBitmapContainer();
            if(!BufferUtil.isBackedBySimpleArray(bc.bitmap))
                throw new RuntimeException("Should not happen. Internal bug.");
            long[] bitArray = bc.bitmap.array();
            if (BufferUtil.isBackedBySimpleArray(value2.content)) {
                short[] sarray = value2.content.array();
                for (int k = 0; k < value2.cardinality; ++k) {
                    short v = sarray[k];
                    final int i = BufferUtil.toIntUnsigned(v) >>> 6;
                    bitArray[i] ^= (1l << v);
                }
            } else
                for (int k = 0; k < value2.cardinality; ++k) {
                    short v2 = value2.content.get(k);
                    final int i = BufferUtil.toIntUnsigned(v2) >>> 6;
                    bitArray[i] ^= (1l << v2);
                }
            if (BufferUtil.isBackedBySimpleArray(this.content)) {
                short[] sarray = this.content.array();
                for (int k = 0; k < this.cardinality; ++k) {
                    short v = sarray[k];
                    final int i = BufferUtil.toIntUnsigned(v) >>> 6;
                    bitArray[i] ^= (1l << v);
                }
            } else
                for (int k = 0; k < this.cardinality; ++k) {
                    short v = this.content.get(k);
                    final int i = BufferUtil.toIntUnsigned(v) >>> 6;
                    bitArray[i] ^= (1l << v);
                }

            bc.cardinality = 0;
            int len = bc.bitmap.limit();
            for (int index = 0; index < len; ++index) {
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
    public MappeableContainer xor(final MappeableRunContainer value2) {
        return value2.xor(this);
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
    // not thread-safe
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
            // TODO: may need to convert to a RunContainer
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
            if (newcardinality > DEFAULT_MAX_SIZE) {
                MappeableBitmapContainer a = this.toBitmapContainer();
                return a.iadd(begin, end);
            }
            MappeableArrayContainer answer = new MappeableArrayContainer(newcardinality, content);
            if(!BufferUtil.isBackedBySimpleArray(answer.content))
                throw new RuntimeException("Should not happen. Internal bug.");
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
        // not thread-safe
        public MappeableContainer iadd(int begin, int end) {
            // TODO: may need to convert to a RunContainer
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
            if (newcardinality > DEFAULT_MAX_SIZE) {
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

        @Override
        protected boolean isArrayBacked() {
            return BufferUtil.isBackedBySimpleArray(this.content);
        }

        @Override
        public MappeableContainer repairAfterLazy() {
            return this;
        }

        @Override
        public boolean intersects(MappeableArrayContainer value2) {
            MappeableArrayContainer value1 = this;
            return  BufferUtil.unsignedIntersects(value1.content,
                    value1.getCardinality(), value2.content,
                    value2.getCardinality());
        }

        @Override
        public boolean intersects(MappeableBitmapContainer x) {
            return x.intersects(this);
        }

        @Override
        public boolean intersects(MappeableRunContainer x) {
            return x.intersects(this);
        }

    @Override
    public MappeableContainer runOptimize() {
        int numRuns = numberOfRuns();
        int sizeAsRunContainer = MappeableRunContainer
                .getArraySizeInBytes(numRuns);
        if (getArraySizeInBytes() > sizeAsRunContainer) {
            return new MappeableRunContainer(this, numRuns); // this could be
                                                             // maybe faster if
                                                             // initial
                                                             // container is a
                                                             // bitmap
        } else {
            return this;
        }
    }


}


final class MappeableArrayContainerShortIterator implements PeekableShortIterator {
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
    public int nextAsInt() {
        return BufferUtil.toIntUnsigned(parent.content.get(pos++));
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


    @Override
    public short peekNext() {
        return parent.content.get(pos);
    }

    @Override
    public void advanceIfNeeded(short minval) {
        pos = BufferUtil.advanceUntil(parent.content, pos - 1, parent.cardinality, minval);
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
    public int nextAsInt() {
        return BufferUtil.toIntUnsigned(parent.content.get(pos--));
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


final class RawArrayContainerShortIterator implements PeekableShortIterator {
    int pos;
    MappeableArrayContainer parent;
    short[] content;

    
    RawArrayContainerShortIterator(MappeableArrayContainer p) {
        parent = p;
        if(!p.isArrayBacked()) throw new RuntimeException("internal bug");
        content = p.content.array();
        pos = 0;
    }


    @Override    
    public short peekNext() {
        return content[pos];
    }
    
    @Override
    public boolean hasNext() {
        return pos < parent.cardinality;
    }

    @Override
    public short next() {
        return content[pos++];
    }

    @Override
    public int nextAsInt() {
        return BufferUtil.toIntUnsigned(content[pos++]);
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

    @Override
    public void advanceIfNeeded(short minval) {
        pos = Util.advanceUntil(content, pos - 1, parent.cardinality, minval);
    }

};

final class RawReverseArrayContainerShortIterator implements ShortIterator {
    int pos;
    MappeableArrayContainer parent;
    short[] content;
    
    
    RawReverseArrayContainerShortIterator(MappeableArrayContainer p) {
        parent = p;
        if(!p.isArrayBacked()) throw new RuntimeException("internal bug");
        content = p.content.array();
        pos = parent.cardinality - 1;
    }
 
    @Override
    public boolean hasNext() {
        return pos >= 0;
    }

    @Override
    public short next() {
        return content[pos--];
    }
    
    
    @Override
    public int nextAsInt() {
        return BufferUtil.toIntUnsigned(content[pos--]);
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

