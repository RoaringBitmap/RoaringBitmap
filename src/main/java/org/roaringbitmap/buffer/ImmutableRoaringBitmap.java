package org.roaringbitmap.buffer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.roaringbitmap.IntIterator;

/**
 * 
 * This class is a base class for org.roaringbitmap.buffer.RoaringArray. As the
 * name suggests, it is immutable. It can also be constructed from a ByteBuffer
 * (useful for memory mapping).
 * 
 * Objects of this class may reside almost entirely in memory-map files.
 * 
 * 
 */
public class ImmutableRoaringBitmap implements Iterable<Integer> {

        /**
         * Constructs a new ImmutableRoaringBitmap. Only meta-data is loaded to
         * RAM. The rest is mapped to the ByteBuffer.
         * 
         * @param b
         *                data source
         */
        public ImmutableRoaringBitmap(final ByteBuffer b) {
                highlowcontainer = new RoaringArray(b);
        }

        protected ImmutableRoaringBitmap() {

        }

        /**
         * Checks whether the value in included, which is equivalent to checking
         * if the corresponding bit is set (get in BitSet class).
         * 
         * @param x
         *                integer value
         * @return whether the integer value is included.
         */
        public boolean contains(final int x) {
                final short hb = Util.highbits(x);
                final Container C = highlowcontainer.getContainer(hb);
                if (C == null)
                        return false;
                return C.contains(Util.lowbits(x));
        }

        /**
         * Deserialize.
         * 
         * @param in
         *                the DataInput stream
         * @throws IOException
         *                 Signals that an I/O exception has occurred.
         */
        public void deserialize(DataInput in) throws IOException {
                this.highlowcontainer.deserialize(in);
        }

        @Override
        public boolean equals(Object o) {
                if (o instanceof ImmutableRoaringBitmap) {
                        final ImmutableRoaringBitmap srb = (ImmutableRoaringBitmap) o;
                        return srb.highlowcontainer
                                .equals(this.highlowcontainer);
                }
                return false;
        }

        /**
         * Returns the number of distinct integers added to the bitmap (e.g.,
         * number of bits set).
         * 
         * @return the cardinality
         */
        public int getCardinality() {
                int size = 0;
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        size += this.highlowcontainer.getContainerAtIndex(i)
                                .getCardinality();
                }
                return size;
        }

        /**
         * Estimate of the memory usage of this data structure. This is not
         * meant to be an exact value.
         * 
         * @return estimated memory usage.
         */
        public int getSizeInBytes() {
                int size = 2;
                for (int i = 0; i < this.highlowcontainer.size(); i++) {
                        final Container c = this.highlowcontainer
                                .getContainerAtIndex(i);
                        size += 4 + c.getSizeInBytes();
                }
                return size;
        }

        @Override
        public int hashCode() {
                return highlowcontainer.hashCode();
        }

        /**
         * iterate over the positions of the true values.
         * 
         * @return the iterator
         */
        @Override
        public Iterator<Integer> iterator() {
                return new Iterator<Integer>() {
                        @Override
                        public boolean hasNext() {
                                return pos < ImmutableRoaringBitmap.this.highlowcontainer
                                        .size();
                        }

                        public Iterator<Integer> init() {
                                if (pos < ImmutableRoaringBitmap.this.highlowcontainer
                                        .size()) {
                                        iter = ImmutableRoaringBitmap.this.highlowcontainer
                                                .getContainerAtIndex(pos)
                                                .iterator();
                                        hs = Util
                                                .toIntUnsigned(ImmutableRoaringBitmap.this.highlowcontainer
                                                        .getKeyAtIndex(pos)) << 16;
                                }
                                return this;
                        }

                        @Override
                        public Integer next() {
                                x = Util.toIntUnsigned(iter.next()) | hs;
                                if (!iter.hasNext()) {
                                        ++pos;
                                        init();
                                }
                                return x;
                        }

                        @Override
                        public void remove() {
                                throw new RuntimeException("Cannot modify.");
                        }

                        int hs = 0;

                        Iterator<Short> iter;

                        short pos = 0;

                        int x;

                }.init();
        }

        /**
         * Serialize.
         * 
         * The current bitmap is not modified.
         * 
         * @param out
         *                the DataOutput stream
         * @throws IOException
         *                 Signals that an I/O exception has occurred.
         */
        public void serialize(DataOutput out) throws IOException {
                this.highlowcontainer.serialize(out);

        }

        /**
         * Return the set values as an array.
         * 
         * @return array representing the set values.
         */
        public int[] toArray() {
                final int[] array = new int[this.getCardinality()];
                int pos = 0, pos2 = 0;
                while (pos < this.highlowcontainer.size()) {
                        final int hs = Util.toIntUnsigned(this.highlowcontainer
                                .getKeyAtIndex(pos)) << 16;
                        final Container C = this.highlowcontainer
                                .getContainerAtIndex(pos++);
                        C.fillLeastSignificant16bits(array, pos2, hs);
                        pos2 += C.getCardinality();
                }
                return array;
        }

        /**
         * A string describing the bitmap.
         * 
         * @return the string
         */
        @Override
        public String toString() {
                final StringBuffer answer = new StringBuffer();
                final IntIterator i = this.getIntIterator();
                answer.append("{");
                if (i.hasNext())
                        answer.append(i.next());
                while (i.hasNext()) {
                        answer.append(",");
                        answer.append(i.next());
                }
                answer.append("}");
                return answer.toString();
        }

        /**
         * Serialize the object.
         * 
         * @param out
         *                output stream
         * @throws IOException
         */
        public void writeExternal(final ObjectOutput out) throws IOException {
                this.highlowcontainer.writeExternal(out);
        }

        private IntIterator getIntIterator() {
                return new IntIterator() {
                        @Override
                        public boolean hasNext() {
                                return pos < ImmutableRoaringBitmap.this.highlowcontainer
                                        .size();
                        }

                        public IntIterator init() {
                                if (pos < ImmutableRoaringBitmap.this.highlowcontainer
                                        .size()) {
                                        iter = ImmutableRoaringBitmap.this.highlowcontainer
                                                .getContainerAtIndex(pos)
                                                .iterator();
                                        hs = Util
                                                .toIntUnsigned(ImmutableRoaringBitmap.this.highlowcontainer
                                                        .getKeyAtIndex(pos)) << 16;
                                }
                                return this;
                        }

                        @Override
                        public int next() {
                                x = Util.toIntUnsigned(iter.next()) | hs;
                                if (!iter.hasNext()) {
                                        ++pos;
                                        init();
                                }
                                return x;
                        }

                        @Override
                        public void remove() {
                                throw new RuntimeException("Cannot modify");
                        }

                        int hs = 0;

                        Iterator<Short> iter;

                        short pos = 0;

                        int x;

                }.init();
        }

        /**
         * Bitwise AND (intersection) operation. The provided bitmaps are *not*
         * modified. This operation is thread-safe as long as the provided
         * bitmaps remain unchanged.
         * 
         * If you have more than 2 bitmaps, consider using the 
         * FastAggregation class.
         * 
         * @param x1
         *                first bitmap
         * @param x2
         *                other bitmap
         * @return result of the operation
         */
        public static RoaringBitmap and(final ImmutableRoaringBitmap x1,
                final ImmutableRoaringBitmap x2) {
                final RoaringBitmap answer = new RoaringBitmap();
                int pos1 = 0, pos2 = 0;
                final int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();
                /*
                 * TODO: This could be optimized quite a bit when one bitmap is
                 * much smaller than the other one.
                 */
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = x1.highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);
                        do {
                                if (s1 < s2) {
                                        pos1++;
                                        if (pos1 == length1)
                                                break main;
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        pos2++;
                                        if (pos2 == length2)
                                                break main;
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        final Container C = x1.highlowcontainer
                                                .getContainerAtIndex(pos1)
                                                .and(x2.highlowcontainer
                                                        .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0)
                                                answer.highlowcontainer.append(
                                                        s1, C);
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2))
                                                break main;
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        } while (true);
                }
                return answer;
        }

        /**
         * Bitwise ANDNOT (difference) operation. The provided bitmaps are *not*
         * modified. This operation is thread-safe as long as the provided
         * bitmaps remain unchanged.
         * 
         * @param x1
         *                first bitmap
         * @param x2
         *                other bitmap
         * @return result of the operation
         */
        public static RoaringBitmap andNot(final ImmutableRoaringBitmap x1,
                final ImmutableRoaringBitmap x2) {
                final RoaringBitmap answer = new RoaringBitmap();
                int pos1 = 0, pos2 = 0;
                final int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = x1.highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);
                        do {
                                if (s1 < s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x1.highlowcontainer, pos1);
                                        pos1++;
                                        if (pos1 == length1)
                                                break main;
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        final Container C = x1.highlowcontainer
                                                .getContainerAtIndex(pos1)
                                                .andNot(x2.highlowcontainer
                                                        .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0)
                                                answer.highlowcontainer.append(
                                                        s1, C);
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2))
                                                break main;
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        } while (true);
                }
                if (pos2 == length2) {
                        answer.highlowcontainer.appendCopy(x1.highlowcontainer,
                                pos1, length1);
                }
                return answer;
        }

        /**
         * Complements the bits in the given range, from rangeStart (inclusive)
         * rangeEnd (exclusive). The given bitmap is unchanged.
         * 
         * @param bm
         *                bitmap being negated
         * @param rangeStart
         *                inclusive beginning of range
         * @param rangeEnd
         *                exclusive ending of range
         * @return a new Bitmap
         */
        public static RoaringBitmap flip(ImmutableRoaringBitmap bm,
                final int rangeStart, final int rangeEnd) {
                RoaringBitmap answer = null;
                if (rangeStart >= rangeEnd) {
                        throw new RuntimeException("Invalid range "
                                + rangeStart + " -- " + rangeEnd);
                }

                answer = new RoaringBitmap();
                final short hbStart = Util.highbits(rangeStart);
                final short lbStart = Util.lowbits(rangeStart);
                final short hbLast = Util.highbits(rangeEnd - 1);
                final short lbLast = Util.lowbits(rangeEnd - 1);

                // copy the containers before the active area
                answer.highlowcontainer.appendCopiesUntil(bm.highlowcontainer,
                        hbStart);

                final int max = Util.toIntUnsigned(Util.maxLowBit());
                for (short hb = hbStart; hb <= hbLast; ++hb) {
                        final int containerStart = (hb == hbStart) ? Util
                                .toIntUnsigned(lbStart) : 0;
                        final int containerLast = (hb == hbLast) ? Util
                                .toIntUnsigned(lbLast) : max;

                        final int i = bm.highlowcontainer.getIndex(hb);
                        final int j = answer.highlowcontainer.getIndex(hb);
                        assert j < 0;

                        if (i >= 0) {
                                final Container c = bm.highlowcontainer
                                        .getContainerAtIndex(i).not(
                                                containerStart, containerLast);
                                if (c.getCardinality() > 0)
                                        answer.highlowcontainer
                                                .insertNewKeyValueAt(-j - 1,
                                                        hb, c);

                        } else { // *think* the range of ones must never be
                                 // empty.
                                answer.highlowcontainer.insertNewKeyValueAt(
                                        -j - 1, hb, Container.rangeOfOnes(
                                                containerStart, containerLast));
                        }
                }
                // copy the containers after the active area.
                answer.highlowcontainer.appendCopiesAfter(bm.highlowcontainer,
                        hbLast);

                return answer;
        }

        /**
         * Bitwise OR (union) operation. The provided bitmaps are *not*
         * modified. This operation is thread-safe as long as the provided
         * bitmaps remain unchanged.
         * 
         * If you have more than 2 bitmaps, consider using the 
         * FastAggregation class.
         * 
         * @param x1
         *                first bitmap
         * @param x2
         *                other bitmap
         * @return result of the operation
         */
        public static RoaringBitmap or(final ImmutableRoaringBitmap x1,
                final ImmutableRoaringBitmap x2) {
                final RoaringBitmap answer = new RoaringBitmap();
                int pos1 = 0, pos2 = 0;
                final int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();
                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = x1.highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);

                        while (true) {
                                if (s1 < s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x1.highlowcontainer, pos1);
                                        pos1++;
                                        if (pos1 == length1) {
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x2.highlowcontainer, pos2);
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        answer.highlowcontainer
                                                .append(s1,
                                                        x1.highlowcontainer
                                                                .getContainerAtIndex(
                                                                        pos1)
                                                                .or(x2.highlowcontainer
                                                                        .getContainerAtIndex(pos2)));
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2)) {
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        }
                }
                if (pos1 == length1) {
                        answer.highlowcontainer.appendCopy(x2.highlowcontainer,
                                pos2, length2);
                } else if (pos2 == length2) {
                        answer.highlowcontainer.appendCopy(x1.highlowcontainer,
                                pos1, length1);
                }
                return answer;
        }

        /**
         * Bitwise XOR (symmetric difference) operation. The provided bitmaps
         * are *not* modified. This operation is thread-safe as long as the
         * provided bitmaps remain unchanged.
         * 
         * If you have more than 2 bitmaps, consider using the 
         * FastAggregation class.
         * 
         * @param x1
         *                first bitmap
         * @param x2
         *                other bitmap
         * @return result of the operation
         */
        public static RoaringBitmap xor(final ImmutableRoaringBitmap x1,
                final ImmutableRoaringBitmap x2) {
                final RoaringBitmap answer = new RoaringBitmap();
                int pos1 = 0, pos2 = 0;
                final int length1 = x1.highlowcontainer.size(), length2 = x2.highlowcontainer
                        .size();

                main: if (pos1 < length1 && pos2 < length2) {
                        short s1 = x1.highlowcontainer.getKeyAtIndex(pos1);
                        short s2 = x2.highlowcontainer.getKeyAtIndex(pos2);

                        while (true) {
                                if (s1 < s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x1.highlowcontainer, pos1);
                                        pos1++;
                                        if (pos1 == length1) {
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                } else if (s1 > s2) {
                                        answer.highlowcontainer.appendCopy(
                                                x2.highlowcontainer, pos2);
                                        pos2++;
                                        if (pos2 == length2) {
                                                break main;
                                        }
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                } else {
                                        final Container C = x1.highlowcontainer
                                                .getContainerAtIndex(pos1)
                                                .xor(x2.highlowcontainer
                                                        .getContainerAtIndex(pos2));
                                        if (C.getCardinality() > 0)
                                                answer.highlowcontainer.append(
                                                        s1, C);
                                        pos1++;
                                        pos2++;
                                        if ((pos1 == length1)
                                                || (pos2 == length2)) {
                                                break main;
                                        }
                                        s1 = x1.highlowcontainer
                                                .getKeyAtIndex(pos1);
                                        s2 = x2.highlowcontainer
                                                .getKeyAtIndex(pos2);
                                }
                        }
                }
                if (pos1 == length1) {
                        answer.highlowcontainer.appendCopy(x2.highlowcontainer,
                                pos2, length2);
                } else if (pos2 == length2) {
                        answer.highlowcontainer.appendCopy(x1.highlowcontainer,
                                pos1, length1);
                }

                return answer;
        }

        protected RoaringArray highlowcontainer = null;

}