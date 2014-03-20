/*
 * Copyright 2013-2014 by Daniel Lemire, Owen Kaser and Samy Chambi
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ShortBuffer;
import java.util.Iterator;

import org.roaringbitmap.ShortIterator;

/**
 * Simple container made of an array of 16-bit integers. 
 * Unlike org.roaringbitmap.ArrayContainer, this
 * class uses a ShortBuffer to store data.  
 * 
 */
public final class ArrayContainer extends Container implements Cloneable,
        Serializable {
        /**
         * Create an array container with default capacity
         */
        public ArrayContainer() {
                this(DEFAULTINITSIZE);
        }

        /**
         * Create an array container with specified capacity
         * 
         * @param capacity
         */
        public ArrayContainer(final int capacity) {
                content = ShortBuffer.allocate(capacity);
        }

        /**
         * Create an array container with a run of ones from firstOfRun to
         * lastOfRun, inclusive. Caller is responsible for making sure the range
         * is small enough that ArrayContainer is appropriate.
         * 
         * @param firstOfRun
         *                first index
         * @param lastOfRun
         *                last index (range is inclusive)
         */
        public ArrayContainer(final int firstOfRun, final int lastOfRun) {
                final int valuesInRange = lastOfRun - firstOfRun + 1;
                content = ShortBuffer.allocate(valuesInRange);
                for (int i = 0; i < valuesInRange; ++i)
                        content.put(i, (short) (firstOfRun + i));
                cardinality = valuesInRange;
        }
        
        /**
         * Construct a new ArrayContainer backed by the provided 
         * ShortBuffer. Note that if you modify the ArrayContainer
         * a new ShortBuffer may be produced.
         * 
         * @param array ShortBuffer where the data is stored
         * @param cardinality cardinality (number of values stored)
         */
        public ArrayContainer(final ShortBuffer array, final int cardinality) {
                this.cardinality = cardinality;
                this.content = array;
        }

        private ArrayContainer(int newcard, ShortBuffer newcontent) {
                this.cardinality = newcard;
                this.content = ShortBuffer.allocate(newcontent.limit());
                newcontent.rewind();
                this.content.put(newcontent);
        }

        /**
         * running time is in O(n) time if insert is not in order.
         * 
         */
        @Override
        public Container add(final short x) {
                // Transform the ArrayContainer to a BitmapContainer
                // when cardinality = DEFAULTMAXSIZE
                if (cardinality >= DEFAULTMAXSIZE) {
                        BitmapContainer a = this.toBitmapContainer();
                        a.add(x);
                        return a;
                }
                if ((cardinality == 0)
                        || (Util.toIntUnsigned(x) > Util
                                .toIntUnsigned(content.get(cardinality - 1)))) {
                        if (cardinality >= this.content.limit())
                                increaseCapacity();
                        content.put(cardinality++, x);
                        return this;
                }
                int loc = Util
                        .unsigned_binarySearch(content, 0, cardinality, x);
                if (loc < 0) {
                        if (cardinality >= this.content.limit())
                                increaseCapacity();
                        // insertion : shift the elements > x by one position to
                        // the right
                        // and put x in it's appropriate place
                        for(int k = cardinality; k > -loc - 1;--k)
                                content.put(k, content.get(k-1));
                        //System.arraycopy(content, -loc - 1, content, -loc,
                        //        cardinality + loc + 1);
                        content.put(-loc - 1, x);
                        ++cardinality;
                }
                return this;
        }

        @Override
        public ArrayContainer and(final ArrayContainer value2) {

                ArrayContainer value1 = this;
                final int desiredcapacity = Math.min(value1.getCardinality(),
                        value2.getCardinality());
                ArrayContainer answer = new ArrayContainer(desiredcapacity);
                answer.cardinality = Util
                        .unsigned_intersect2by2(value1.content,
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
                final int desiredcapacity = value1.getCardinality();
                ArrayContainer answer = new ArrayContainer(desiredcapacity);
                answer.cardinality = Util.unsigned_difference(value1.content,
                        value1.getCardinality(), value2.content,
                        value2.getCardinality(), answer.content);
                return answer;
        }

        @Override
        public ArrayContainer andNot(BitmapContainer value2) {
                final ArrayContainer answer = new ArrayContainer(content.limit());
                int pos = 0;
                for (int k = 0; k < cardinality; ++k)
                        if (!value2.contains(this.content.get(k)))
                                answer.content.put(pos++, this.content.get(k));
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
                return Util.unsigned_binarySearch(content, 0, cardinality, x) >= 0;
        }

        @Override
        public boolean equals(Object o) {
                if (o instanceof ArrayContainer) {
                        ArrayContainer srb = (ArrayContainer) o;
                        if (srb.cardinality != this.cardinality)
                                return false;
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
                for (int k = 0; k < this.cardinality; ++k)
                        x[k + i] = Util.toIntUnsigned(this.content.get(k)) | mask;

        }

        @Override
        public int getCardinality() {
                return cardinality;
        }

        @Override
        public ShortIterator getShortIterator() {
                return new ShortIterator() {
                        @Override
                        public boolean hasNext() {
                                return pos < ArrayContainer.this.cardinality;
                        }

                        @Override
                        public short next() {
                                return ArrayContainer.this.content.get(pos++);
                        }

                        @Override
                        public void remove() {
                                ArrayContainer.this.remove((short) (pos - 1));
                                pos--;
                        }

                        int pos = 0;
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
                        hash += 31 * content.get(k);
                return hash;
        }

        @Override
        public ArrayContainer iand(final ArrayContainer value2) {
                ArrayContainer value1 = this;
                value1.cardinality = Util
                        .unsigned_intersect2by2(value1.content,
                                value1.getCardinality(), value2.content,
                                value2.getCardinality(), value1.content);
                return this;
        }

        @Override
        public Container iand(BitmapContainer value2) {
                int pos = 0;
                for (int k = 0; k < cardinality; ++k)
                        if (value2.contains(this.content.get(k)))
                                this.content.put(pos++, this.content.get(k));
                cardinality = pos;
                return this;
        }

        @Override
        public ArrayContainer iandNot(final ArrayContainer value2) {
                this.cardinality = Util.unsigned_difference(this.content,
                        this.getCardinality(), value2.content,
                        value2.getCardinality(), this.content);
                return this;
        }

        @Override
        public ArrayContainer iandNot(BitmapContainer value2) {
                int pos = 0;
                for (int k = 0; k < cardinality; ++k)
                        if (!value2.contains(this.content.get(k)))
                                this.content.put(pos++, this.content.get(k));
                this.cardinality = pos;
                return this;
        }

        @Override
        public Container inot(final int firstOfRange, final int lastOfRange) {
                // determine the span of array indices to be affected
                int startIndex = Util.unsigned_binarySearch(content, 0,
                        cardinality, (short) firstOfRange);
                if (startIndex < 0)
                        startIndex = -startIndex - 1;
                int lastIndex = Util.unsigned_binarySearch(content, 0,
                        cardinality, (short) lastOfRange);
                if (lastIndex < 0)
                        lastIndex = -lastIndex - 1 - 1;
                final int currentValuesInRange = lastIndex - startIndex + 1;
                final int spanToBeFlipped = lastOfRange - firstOfRange + 1;
                final int newValuesInRange = spanToBeFlipped
                        - currentValuesInRange;
                final ShortBuffer buffer = ShortBuffer.allocate(newValuesInRange);
                final int cardinalityChange = newValuesInRange
                        - currentValuesInRange;
                final int newCardinality = cardinality + cardinalityChange;

                if (cardinalityChange > 0) { // expansion, right shifting needed
                        if (newCardinality > content.limit()) {
                                // so big we need a bitmap?
                                if (newCardinality >= DEFAULTMAXSIZE)
                                        return toBitmapContainer().inot(
                                                firstOfRange, lastOfRange);
                                ShortBuffer co = ShortBuffer.allocate(newCardinality);
                                content.rewind();
                                co.put(content);
                                content = co;
                        }
                        // slide right the contentsafter the range
                        for (int pos = cardinality - 1; pos > lastIndex; --pos)
                                content.put(pos + cardinalityChange, content.get(pos));
                        negateRange(buffer, startIndex, lastIndex,
                                firstOfRange, lastOfRange);
                } else { // no expansion needed
                        negateRange(buffer, startIndex, lastIndex,
                                firstOfRange, lastOfRange);
                        if (cardinalityChange < 0) // contraction, left sliding.
                                                   // Leave array oversize
                                for (int i = startIndex + newValuesInRange; i < newCardinality; ++i)
                                        content.put(i,content.get(i
                                                - cardinalityChange));
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
                        @Override
                        public boolean hasNext() {
                                return pos < ArrayContainer.this.cardinality;
                        }

                        @Override
                        public Short next() {
                                return new Short(
                                        ArrayContainer.this.content.get(pos++));
                        }

                        @Override
                        public void remove() {
                                ArrayContainer.this.remove((short) (pos - 1));
                                pos--;
                        }

                        short pos = 0;
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

        // shares lots of code with inot; candidate for refactoring
        @Override
        public Container not(final int firstOfRange, final int lastOfRange) {
                if (firstOfRange > lastOfRange) {
                        return clone(); // empty range
                }

                // determine the span of array indices to be affected
                int startIndex = Util.unsigned_binarySearch(content, 0,
                        cardinality, (short) firstOfRange);
                if (startIndex < 0)
                        startIndex = -startIndex - 1;
                int lastIndex = Util.unsigned_binarySearch(content, 0,
                        cardinality, (short) lastOfRange);
                if (lastIndex < 0)
                        lastIndex = -lastIndex - 2;
                final int currentValuesInRange = lastIndex - startIndex + 1;
                final int spanToBeFlipped = lastOfRange - firstOfRange + 1;
                final int newValuesInRange = spanToBeFlipped
                        - currentValuesInRange;
                final int cardinalityChange = newValuesInRange
                        - currentValuesInRange;
                final int newCardinality = cardinality + cardinalityChange;

                if (newCardinality >= DEFAULTMAXSIZE)
                        return toBitmapContainer().not(firstOfRange,
                                lastOfRange);

                ArrayContainer answer = new ArrayContainer();
                if (answer.content.limit() < newCardinality)
                        answer.content = ShortBuffer.allocate(newCardinality);

                for (int i = 0; i < startIndex; ++i)
                        // copy stuff before the active area
                        answer.content.put(i,content.get(i));

                int outPos = startIndex;
                int inPos = startIndex; // item at inPos always >= valInRange

                int valInRange = firstOfRange;
                for (; valInRange <= lastOfRange && inPos <= lastIndex; ++valInRange) {
                        if ((short) valInRange != content.get(inPos))
                                answer.content.put(outPos++, (short) valInRange);
                        else {
                                ++inPos;
                        }
                }

                for (; valInRange <= lastOfRange; ++valInRange) {
                        answer.content.put(outPos++,(short) valInRange);
                }

                // content after the active range
                for (int i = lastIndex + 1; i < cardinality; ++i)
                        answer.content.put(outPos++, content.get(i));
                answer.cardinality = newCardinality;
                return answer;
        }

        @Override
        public Container or(final ArrayContainer value2) {
                final ArrayContainer value1 = this;
                int totalCardinality = value1.getCardinality()
                        + value2.getCardinality();
                if (totalCardinality > DEFAULTMAXSIZE) {// it could be a bitmap!
                        BitmapContainer bc = new BitmapContainer();
                        for (int k = 0; k < value2.cardinality; ++k) {
                                final int i = Util
                                        .toIntUnsigned(value2.content.get(k)) >>> 6;
                                bc.bitmap.put(i, bc.bitmap.get(i) | (1l << value2.content.get(k)));
                        }
                        for (int k = 0; k < this.cardinality; ++k) {
                                final int i = Util
                                        .toIntUnsigned(this.content.get(k)) >>> 6;
                                bc.bitmap.put(i, bc.bitmap.get(i) | (1l << this.content.get(k)));
                        }
                        bc.cardinality = 0;
                        for(int index = 0; index < bc.bitmap.limit(); ++index) {
                                bc.cardinality += Long.bitCount(bc.bitmap.get(index));
                        }
                        if (bc.cardinality <= DEFAULTMAXSIZE)
                                return bc.toArrayContainer();
                        return bc;
                }
                final int desiredcapacity = totalCardinality; // Math.min(BitmapContainer.maxcapacity,
                                                              // totalCardinality);
                ArrayContainer answer = new ArrayContainer(desiredcapacity);
                answer.cardinality = Util.unsigned_union2by2(value1.content,
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
                byte[] buffer = new byte[2];
                // little endian
                in.readFully(buffer);
                this.cardinality = (buffer[0] & 0xFF) | ((buffer[1] & 0xFF) << 8);
                if (this.content.limit() < this.cardinality)
                        this.content = ShortBuffer.allocate(this.cardinality);
                for (int k = 0; k < this.cardinality; ++k) {
                        in.readFully(buffer);
                        this.content.put(k, (short) (((buffer[1] & 0xFF) << 8) | (buffer[0] & 0xFF)));
                }
        }

        @Override
        public Container remove(final short x) {
                final int loc = Util.unsigned_binarySearch(content, 0,
                        cardinality, x);
                if (loc >= 0) {
                        // insertion
                        System.arraycopy(content, loc + 1, content, loc,
                                cardinality - loc - 1);
                        --cardinality;
                }
                return this;
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
                StringBuffer sb = new StringBuffer();
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
                ShortBuffer co = ShortBuffer.allocate(this.cardinality);
                for(int k = 0; k < this.cardinality; ++k)
                        co.put(this.content.get(k));
                this.content = co;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
                out.write((this.cardinality >>> 0) & 0xFF);
                out.write((this.cardinality >>> 8) & 0xFF);
                // little endian
                for (int k = 0; k < this.cardinality; ++k) {
                        out.write((this.content.get(k) >>> 0) & 0xFF);
                        out.write((this.content.get(k) >>> 8) & 0xFF);
                }
        }

        @Override
        public Container xor(final ArrayContainer value2) {
                final ArrayContainer value1 = this;
                final int totalCardinality = value1.getCardinality()
                        + value2.getCardinality();
                if (totalCardinality > DEFAULTMAXSIZE) {// it could be a bitmap!
                        BitmapContainer bc = new BitmapContainer();
                        for (int k = 0; k < value2.cardinality; ++k) {
                                final int i = Util
                                        .toIntUnsigned(value2.content.get(k)) >>> 6;
                                bc.bitmap.put(i, bc.bitmap.get(i) ^  (1l << value2.content.get(k)));
                        }
                        for (int k = 0; k < this.cardinality; ++k) {
                                final int i = Util
                                        .toIntUnsigned(this.content.get(k)) >>> 6;
                                bc.bitmap.put(i, bc.bitmap.get(i) ^ (1l << this.content.get(k)));
                        }
                        bc.cardinality = 0;
                        for(int index = 0; index < bc.bitmap.limit(); ++index) {
                                bc.cardinality += Long.bitCount(bc.bitmap.get(index));
                        }
                        if (bc.cardinality <= DEFAULTMAXSIZE)
                                return bc.toArrayContainer();
                        return bc;
                }
                final int desiredcapacity = totalCardinality;
                ArrayContainer answer = new ArrayContainer(desiredcapacity);
                answer.cardinality = Util
                        .unsigned_exclusiveunion2by2(value1.content,
                                value1.getCardinality(), value2.content,
                                value2.getCardinality(), answer.content);
                return answer;
        }

        @Override
        public Container xor(BitmapContainer x) {
                return x.xor(this);
        }

        private void increaseCapacity() {
                int newcapacity = this.content.limit() < 64 ? this.content.limit() * 2
                        : this.content.limit() < 1024 ? this.content.limit() * 3 / 2
                                : this.content.limit() * 5 / 4;
                if (newcapacity > ArrayContainer.DEFAULTMAXSIZE)
                        newcapacity = ArrayContainer.DEFAULTMAXSIZE;
                ShortBuffer newcontent = ShortBuffer.allocate(newcapacity);
                this.content.rewind();
                newcontent.put(this.content);
                this.content = newcontent;
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
                for (; valInRange <= lastRange && inPos <= lastIndex; ++valInRange) {
                        if ((short) valInRange != content.get(inPos))
                                buffer.put(outPos++, (short) valInRange);
                        else {
                                ++inPos;
                        }
                }

                // if there are extra items (greater than the biggest
                // pre-existing one in range), buffer them
                for (; valInRange <= lastRange; ++valInRange) {
                        buffer.put(outPos++,(short) valInRange);
                }

                if (outPos != buffer.limit()) {
                        throw new RuntimeException("negateRange: outPos "
                                + outPos + " whereas buffer.length="
                                + buffer.limit());
                }
                assert outPos == buffer.limit();
                // copy back from buffer...caller must ensure there is room
                int i = startIndex;
                for(int k = 0; k < buffer.limit(); ++k) {
                        short item = buffer.get(k);
                        content.put(i++, item);
                }
        }

        protected void loadData(final BitmapContainer bitmapContainer) {
                this.cardinality = bitmapContainer.cardinality;
                bitmapContainer.fillArray(content);
        }

        @Override
        protected void writeArray(ObjectOutput out) throws IOException {
             // little endian
                for (int k = 0; k < this.cardinality; ++k) {
                        out.write((this.content.get(k) >>> 0) & 0xFF);
                        out.write((this.content.get(k) >>> 8) & 0xFF);
                }
        }

        protected int cardinality = 0;

        protected ShortBuffer content;

        private static final int DEFAULTINITSIZE = 4;

        private static final long serialVersionUID = 1L;

        protected static final int DEFAULTMAXSIZE = 4096;

}
