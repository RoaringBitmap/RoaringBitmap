/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;


/**
 * Simple container made of an array of 16-bit integers
 */
public final class ArrayContainer extends Container implements Cloneable {
    private static final int DEFAULT_INIT_SIZE = 4;

    static final int DEFAULT_MAX_SIZE = 4096;// containers with DEFAULT_MAX_SZE or less integers should be ArrayContainers

    protected int cardinality = 0;
    
    private static final long serialVersionUID = 1L;

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
     * @param lastOfRun  last index (range is exclusive)
     */
    public ArrayContainer(final int firstOfRun, final int lastOfRun) {
        final int valuesInRange = lastOfRun - firstOfRun ;
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

    @Override
    int numberOfRuns() {
        if(cardinality == 0)
            return 0; // should never happen
        int numRuns = 1;
        int oldv = Util.toIntUnsigned(content[0]);
        for (int i = 1; i < cardinality ; i++) {
            int newv = Util.toIntUnsigned(content[i]);
            if(oldv + 1 != newv) ++numRuns;
            oldv = newv;
        }
        return numRuns;
    }



  

    /**
     * running time is in O(n) time if insert is not in order.
     */
    @Override
    public Container add(final short x) {
        int loc = Util.unsignedBinarySearch(content, 0, cardinality, x);
        if (loc < 0) {
            // Transform the ArrayContainer to a BitmapContainer
            // when cardinality = DEFAULT_MAX_SIZE
            if (cardinality >= DEFAULT_MAX_SIZE) {
                BitmapContainer a = this.toBitmapContainer();
                a.add(x);
                return a;
            }
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
    public int andCardinality(final ArrayContainer value2) {
        return Util.unsignedLocalIntersect2by2Cardinality(content,
                cardinality, value2.content,
                value2.getCardinality());
    }
    
    @Override
    public Container and(BitmapContainer x) {
        return x.and(this);
    }

    @Override
    public int andCardinality(BitmapContainer x) {
        return x.andCardinality(this);
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
        for (int k = 0; k < cardinality; ++k) {
            short val = this.content[k];
            if (!value2.contains(val))
                answer.content[pos++] = val;
        }
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
        this.cardinality = 0xFFFF & Short.reverseBytes(in.readShort());
        if (this.content.length < this.cardinality)
            this.content = new short[this.cardinality];
        for (int k = 0; k < this.cardinality; ++k) {
            this.content[k] = Short.reverseBytes(in.readShort());;
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
        } else if (o instanceof RunContainer) {
            return o.equals(this);
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
        return new ArrayContainerShortIterator(this);
    }
    

    @Override
    public ShortIterator getReverseShortIterator() {
        return new ReverseArrayContainerShortIterator(this);
    }
    
    @Override
    public int getSizeInBytes() {
        return this.cardinality * 2 + 4;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (int k = 0; k < cardinality; ++k)
            hash += 31 * hash + content[k];
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
        for (int k = 0; k < cardinality; ++k) {
            short v = this.content[k];
            if (value2.contains(v))
                this.content[pos++] = v;
        }
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
        for (int k = 0; k < cardinality; ++k) {
            short v = this.content[k];
            if (!value2.contains(v))
                this.content[pos++] = v;
        }
        this.cardinality = pos;
        return this;
    }


    private void increaseCapacity() { 
        increaseCapacity(false);
    }

    // temporarily allow an illegally large size, as long as the operation creating
    // the illegal container does not return it.
    private void increaseCapacity(boolean allowIllegalSize) {
        int newCapacity = (this.content.length == 0) ? DEFAULT_INIT_SIZE : this.content.length < 64 ? this.content.length * 2
                : this.content.length < 1067 ? this.content.length * 3 / 2
                : this.content.length * 5 / 4;
        // never allocate more than we will ever need
        if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE  && ! allowIllegalSize)
            newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
        // if we are within 1/16th of the max, go to max 
        if( newCapacity < ArrayContainer.DEFAULT_MAX_SIZE - ArrayContainer.DEFAULT_MAX_SIZE/16)
            newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
        this.content = Arrays.copyOf(this.content, newCapacity);
    }

    private void increaseCapacity(int min) {
        int newCapacity = (this.content.length == 0) ? DEFAULT_INIT_SIZE : this.content.length < 64 ? this.content.length * 2
                : this.content.length < 1024 ? this.content.length * 3 / 2
                : this.content.length * 5 / 4;
        if(newCapacity < min) newCapacity = min;
        // never allocate more than we will ever need
        if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE)
            newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
        // if we are within 1/16th of the max, go to max 
        if( newCapacity < ArrayContainer.DEFAULT_MAX_SIZE - ArrayContainer.DEFAULT_MAX_SIZE/16)
            newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
        this.content = Arrays.copyOf(this.content, newCapacity);
    }

    
    @Override
    public Container inot(final int firstOfRange, final int lastOfRange) {
        // TODO: may need to convert to a RunContainer
        // determine the span of array indices to be affected
        int startIndex = Util.unsignedBinarySearch(content, 0, cardinality, (short) firstOfRange);
        if (startIndex < 0)
            startIndex = -startIndex - 1;
        int lastIndex = Util.unsignedBinarySearch(content, 0, cardinality, (short) (lastOfRange-1));
        if (lastIndex < 0)
            lastIndex = -lastIndex - 1 - 1;
        final int currentValuesInRange = lastIndex - startIndex + 1;
        final int spanToBeFlipped = lastOfRange - firstOfRange ;
        final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
        final short[] buffer = new short[newValuesInRange];
        final int cardinalityChange = newValuesInRange - currentValuesInRange;
        final int newCardinality = cardinality + cardinalityChange;

        if (cardinalityChange > 0) { // expansion, right shifting needed
            if (newCardinality > content.length) {
                // so big we need a bitmap?
                if (newCardinality > DEFAULT_MAX_SIZE)
                    return toBitmapContainer().inot(firstOfRange, lastOfRange);
                content = Arrays.copyOf(content, newCardinality);
            }
            // slide right the contents after the range
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
        for (; valInRange < lastRange && inPos <= lastIndex; ++valInRange) {
            if ((short) valInRange != content[inPos]) {
                buffer[outPos++] = (short) valInRange;
            } else {
                ++inPos;
            }
        }

        // if there are extra items (greater than the biggest
        // pre-existing one in range), buffer them
        for (; valInRange < lastRange; ++valInRange) {
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
        // TODO: may need to convert to a RunContainer
        if (firstOfRange >= lastOfRange) {
            return clone(); // empty range
        }

        // determine the span of array indices to be affected
        int startIndex = Util.unsignedBinarySearch(content, 0,
                cardinality, (short) firstOfRange);
        if (startIndex < 0)
            startIndex = -startIndex - 1;
        int lastIndex = Util.unsignedBinarySearch(content, 0,
                cardinality, (short) (lastOfRange - 1));
        if (lastIndex < 0)
            lastIndex = -lastIndex - 2;
        final int currentValuesInRange = lastIndex - startIndex + 1;
        final int spanToBeFlipped = lastOfRange - firstOfRange;
        final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
        final int cardinalityChange = newValuesInRange - currentValuesInRange;
        final int newCardinality = cardinality + cardinalityChange;

        if (newCardinality > DEFAULT_MAX_SIZE)
            return toBitmapContainer().not(firstOfRange, lastOfRange);

        ArrayContainer answer = new ArrayContainer(newCardinality);

        // copy stuff before the active area
        System.arraycopy(content, 0, answer.content, 0, startIndex);

        int outPos = startIndex;
        int inPos = startIndex; // item at inPos always >= valInRange

        int valInRange = firstOfRange;
        for (; valInRange < lastOfRange && inPos <= lastIndex; ++valInRange) {
            if ((short) valInRange != content[inPos]) {
                answer.content[outPos++] = (short) valInRange;
            } else {
                ++inPos;
            }
        }

        for (; valInRange < lastOfRange; ++valInRange) {
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
                short v = value2.content[k];
                final int i = Util.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] |= (1l << v);
            }
            for (int k = 0; k < this.cardinality; ++k) {
                short v = this.content[k];
                final int i = Util.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] |= (1l << v);
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

   
    private int advance(ShortIterator it) {
        if (it.hasNext()) 
           return  Util.toIntUnsigned(it.next());
        else
            return -1;
    }

    // in order 
    private void emit(short val) {
        if (cardinality == content.length)
            increaseCapacity(true);
        content[cardinality++] = val;
    }

        
    /** it must return items in (unsigned) sorted order.  Possible candidate for
        Container interface?  **/
    private Container or(ShortIterator it, final boolean exclusive) {
        ArrayContainer ac = new ArrayContainer();
        int myItPos = 0;
        ac.cardinality=0;
        // do a merge.  int -1 denotes end of input.
        int myHead = (myItPos == cardinality) ? -1 : Util.toIntUnsigned(content[myItPos++]);
        int hisHead =  advance(it);

        while (myHead != -1 && hisHead != -1) {
            if (myHead < hisHead) {
                ac.emit( (short) myHead);
                myHead = (myItPos == cardinality) ? -1 : Util.toIntUnsigned(content[myItPos++]);
            }
            else if (myHead > hisHead) {
                ac.emit(  (short) hisHead);
                hisHead = advance(it);
            }
            else {
                if (! exclusive) 
                    ac.emit( (short) hisHead);
                hisHead = advance(it);
                myHead = (myItPos == cardinality) ? -1 : Util.toIntUnsigned(content[myItPos++]);
            }
        }

        while (myHead != -1) {
            ac.emit( (short) myHead);
            myHead = (myItPos == cardinality) ? -1 : Util.toIntUnsigned(content[myItPos++]);
        }

        while (hisHead != -1) {
            ac.emit( (short) hisHead);
            hisHead = advance(it);
        }

        if (ac.cardinality > DEFAULT_MAX_SIZE)
            return ac.toBitmapContainer();
        else return ac;
    }

    protected Container or(ShortIterator it) {
        return or(it, false);
    }

    protected Container xor(ShortIterator it) {
        return or(it,true);
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
        out.writeShort(Short.reverseBytes((short) this.cardinality));
        // little endian
        for (int k = 0; k < this.cardinality; ++k) {
            out.writeShort(Short.reverseBytes((short) this.content[k]));
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
        if(this.content.length == this.cardinality) return;
        this.content = Arrays.copyOf(this.content, this.cardinality);
    }

    @Override
    protected void writeArray(DataOutput out) throws IOException {
        // little endian
        for (int k = 0; k < this.cardinality; ++k) {
            short v = this.content[k];
            out.write(v & 0xFF);
            out.write((v >>> 8) & 0xFF);
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
                short v = value2.content[k];
                final int i = Util.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] ^= (1l << v);
            }
            for (int k = 0; k < this.cardinality; ++k) {
                short v = this.content[k];
                final int i = Util.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] ^= (1l << v);
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

    @Override
    public int rank(short lowbits) {
        int answer =  Util.unsignedBinarySearch(content, 0, cardinality, lowbits);
        if (answer >= 0) {
            return answer + 1;
        } else {
            return -answer - 1;
        }
    }

    @Override
    public short select(int j) {
        return this.content[j];
    }

    @Override
    public Container limit(int maxcardinality) {
        if (maxcardinality < this.getCardinality())
            return new ArrayContainer(maxcardinality, this.content);
        else
            return clone();
    }

    @Override
    public Container iadd(int begin, int end) {
        // TODO: may need to convert to a RunContainer
        int indexstart = Util.unsignedBinarySearch(content, 0, cardinality,
                (short) begin);
        if (indexstart < 0)
            indexstart = -indexstart - 1;
        int indexend = Util.unsignedBinarySearch(content, 0, cardinality,
                (short) (end - 1));
        if (indexend < 0)
            indexend = -indexend - 1;
        else indexend++;
        int rangelength = end - begin;
        int newcardinality = indexstart + (cardinality - indexend) + rangelength;
        if (newcardinality > DEFAULT_MAX_SIZE) {
            BitmapContainer a = this.toBitmapContainer();
            return a.iadd(begin, end);
        }
        if (newcardinality >= this.content.length)
            increaseCapacity(newcardinality);
        System.arraycopy(content, indexend, content, indexstart + rangelength,
                cardinality - indexend);
        for (int k = 0; k <  rangelength; ++k) {
            content[k+indexstart] = (short) (begin + k);
        }
        cardinality = newcardinality;
        return this;
    }

    @Override
    public Container iremove(int begin, int end) {
        int indexstart = Util.unsignedBinarySearch(content, 0, cardinality,
                (short) begin);
        if (indexstart < 0)
            indexstart = -indexstart - 1;
        int indexend = Util.unsignedBinarySearch(content, 0, cardinality,
                (short) (end - 1));
        if (indexend < 0)
            indexend = -indexend - 1;
        else
            indexend++;
        int rangelength = indexend - indexstart;
        System.arraycopy(content, indexstart + rangelength, content, indexstart,
                cardinality - indexstart - rangelength);
        cardinality -= rangelength;
        return this;
    }

    @Override
    public Container flip(short x) {
        int loc = Util.unsignedBinarySearch(content, 0, cardinality, x);
        if (loc < 0) {
            // Transform the ArrayContainer to a BitmapContainer
            // when cardinality = DEFAULT_MAX_SIZE
            if (cardinality >= DEFAULT_MAX_SIZE) {
                BitmapContainer a = this.toBitmapContainer();
                a.add(x);
                return a;
            }
            if (cardinality >= this.content.length)
                increaseCapacity();
            // insertion : shift the elements > x by one position to
            // the right
            // and put x in it's appropriate place
            System.arraycopy(content, -loc - 1, content, -loc, cardinality
                    + loc + 1);
            content[-loc - 1] = x;
            ++cardinality;
        } else {
            System.arraycopy(content, loc + 1, content, loc, cardinality - loc
                    - 1);
            --cardinality;
        }
        return this;
    }

    @Override
    public Container add(int begin, int end) {
        // TODO: may need to convert to a RunContainer
        int indexstart = Util.unsignedBinarySearch(content, 0, cardinality,
                (short) begin);
        if (indexstart < 0)
            indexstart = -indexstart - 1;
        int indexend = Util.unsignedBinarySearch(content, 0, cardinality,
                (short) (end - 1));
        if (indexend < 0)
            indexend = -indexend - 1;
        else
            indexend++;
        int rangelength = end - begin;
        int newcardinality = indexstart + (cardinality - indexend)
                + rangelength;
        if (newcardinality > DEFAULT_MAX_SIZE) {
            BitmapContainer a = this.toBitmapContainer();
            return a.iadd(begin, end);
        }
        ArrayContainer answer = new ArrayContainer(newcardinality, content);
        System.arraycopy(content, indexend, answer.content, indexstart
                + rangelength, cardinality - indexend);
        for (int k = 0; k < rangelength; ++k) {
            answer.content[k + indexstart] = (short) (begin + k);
        }
        answer.cardinality = newcardinality;
        return answer;
    }

    @Override
    public Container remove(int begin, int end) {
        int indexstart = Util.unsignedBinarySearch(content, 0, cardinality,
                (short) begin);
        if (indexstart < 0)
            indexstart = -indexstart - 1;
        int indexend = Util.unsignedBinarySearch(content, 0, cardinality,
                (short) (end - 1));
        if (indexend < 0)
            indexend = -indexend - 1;
        else
            indexend++;
        int rangelength = indexend - indexstart;
        ArrayContainer answer = clone();
        System.arraycopy(content, indexstart + rangelength, answer.content,
                indexstart, cardinality - indexstart - rangelength);
        answer.cardinality = cardinality - rangelength;
        return answer;
    }

    @Override
    // see andNot for an approach that might be better.
    public Container and(RunContainer x) {
        return x.and(this);
    }

    @Override
    // see andNot for an approach that might be better.
    public int andCardinality(RunContainer x) {
        return x.andCardinality(this);
    }
    
    @Override
    public Container andNot(RunContainer x) {
        int writeLocation=0;
        int runStart, runEnd;  // the current or upcoming run.
        int whichRun;
        short [] buffer = new short[ cardinality];
        if (x.nbrruns == 0) return clone();
        else {
            runStart = Util.toIntUnsigned(x.getValue(0));
            runEnd = runStart + Util.toIntUnsigned(x.getLength(0));
            whichRun=0;
        }

        short val;
        for (int i = 0; i < cardinality; ++i) {
            val = content[i];
            int valInt = Util.toIntUnsigned(val);
            if ( valInt < runStart) {
                buffer[writeLocation++] = val;
            }
            else if (valInt <= runEnd)
                ; // don't want item
            else {
                // greater than this run, need to do an advanceUntil on runs
                // done sequentially for now (no galloping attempts).
                do {
                    if (whichRun+1 < x.nbrruns) {
                        whichRun++;
                        runStart = Util.toIntUnsigned(x.getValue(whichRun));
                        runEnd = runStart + Util.toIntUnsigned(x.getLength(whichRun));
                    }
                    else runStart = runEnd = (1 << 16) + 1;  // infinity....
                } while ( valInt > runEnd);
                --i;  // need to re-process this val
            }
        }
        return new ArrayContainer(writeLocation,buffer);
    }

    @Override
    public Container iand(RunContainer x) {
        // possible performance issue, not taking advantage of possible inplace 
        return x.and(this);
    }

    @Override
    public Container iandNot(RunContainer x) {
        // possible performance issue, not taking advantage of possible inplace 
        // could adapt algo above
        return andNot(x);
    }

    @Override
    public Container ior(RunContainer x) {
        // possible performance issue, not taking advantage of possible inplace 
        return x.or(this);
    }

    @Override
    public Container ixor(RunContainer x) {
        // possible performance issue, not taking advantage of possible inplace 
        return x.xor(this);
    }

    @Override
    public Container or(RunContainer x) {
        return x.or(this);
    }

    @Override
    public Container xor(RunContainer x) {
        return x.xor(this);
    }

    @Override
    public Container repairAfterLazy() {
        return this;
    }

    @Override
    public boolean intersects(ArrayContainer value2) {
        ArrayContainer value1 = this;
        return  Util.unsignedIntersects(value1.content,
                value1.getCardinality(), value2.content,
                value2.getCardinality());
    }

    @Override
    public boolean intersects(BitmapContainer x) {
        return x.intersects(this);
    }

    @Override
    public boolean intersects(RunContainer x) {
        return x.intersects(this);
    }

    @Override
    public Container runOptimize() {
        // TODO: consider borrowing the BitmapContainer idea of early
        // abandonment
        // with ArrayContainers, when the number of runs in the arrayContainer
        // passes some threshold based on the cardinality.
        int numRuns = numberOfRuns();
        int sizeAsRunContainer = RunContainer.serializedSizeInBytes(numRuns);
        if (getArraySizeInBytes() > sizeAsRunContainer) {
            return new RunContainer(this, numRuns); // this could be maybe
                                                    // faster if initial
                                                    // container is a bitmap
        } else {
            return this;
        }
    }
   

}


final class ArrayContainerShortIterator implements PeekableShortIterator {
    int pos;
    ArrayContainer parent;
    
    ArrayContainerShortIterator() {
    }
    
    ArrayContainerShortIterator(ArrayContainer p) {
        wrap(p);
    }

    @Override
    public short peekNext() {
        return parent.content[pos];
    }
    

    @Override
    public void advanceIfNeeded(short minval) {
        pos = Util.advanceUntil(parent.content, pos - 1, parent.cardinality, minval);
    }
    
    void wrap(ArrayContainer p) {
        parent = p;
        pos = 0;
    }

    @Override
    public boolean hasNext() {
        return pos < parent.cardinality;
    }

    @Override
    public short next() {
        return parent.content[pos++];
    }

    @Override
    public int nextAsInt() {
        return Util.toIntUnsigned(parent.content[pos++]);
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

};

final class ReverseArrayContainerShortIterator implements ShortIterator {
    int pos;
    ArrayContainer parent;
    
    ReverseArrayContainerShortIterator() {
    }
    
    ReverseArrayContainerShortIterator(ArrayContainer p) {
        wrap(p);
    }
    
    void wrap(ArrayContainer p) {
        parent = p;
        pos = parent.cardinality - 1;
    }

    @Override
    public boolean hasNext() {
        return pos >= 0;
    }

    @Override
    public short next() {
        return parent.content[pos--];
    }
    
    @Override
    public int nextAsInt() {
        return Util.toIntUnsigned(parent.content[pos--]);
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
