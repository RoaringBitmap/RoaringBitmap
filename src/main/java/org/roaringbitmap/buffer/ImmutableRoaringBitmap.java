/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;

import org.roaringbitmap.IntIterator;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * This class is a base class for org.roaringbitmap.buffer.RoaringArray. As the
 * name suggests, it is immutable. It can also be constructed from a ByteBuffer
 * (useful for memory mapping).
 * <p/>
 * Objects of this class may reside almost entirely in memory-map files.
 */
public class ImmutableRoaringBitmap implements Iterable<Integer> {

    /**
     * Bitwise AND (intersection) operation. The provided bitmaps are *not*
     * modified. This operation is thread-safe as long as the provided
     * bitmaps remain unchanged.
     * <p/>
     * If you have more than 2 bitmaps, consider using the
     * FastAggregation class.
     *
     * @param x1 first bitmap
     * @param x2 other bitmap
     * @return result of the operation
     */
    public static RoaringBitmap and(final ImmutableRoaringBitmap x1,
                                    final ImmutableRoaringBitmap x2) {
        final RoaringBitmap answer = new RoaringBitmap();
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer
                .size();
                /*
                 * TODO: This could be optimized quite a bit when one bitmap is
                 * much smaller than the other one.
                 */
        main:
        if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            do {
                if (s1 < s2) {
                    pos1++;
                    if (pos1 == length1)
                        break main;
                    s1 = x1.highLowContainer
                            .getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    pos2++;
                    if (pos2 == length2)
                        break main;
                    s2 = x2.highLowContainer
                            .getKeyAtIndex(pos2);
                } else {
                    final Container c = x1.highLowContainer.getContainerAtIndex(pos1).and(
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0)
                        answer.highLowContainer.append(s1, c);
                    pos1++;
                    pos2++;
                    if ((pos1 == length1)
                            || (pos2 == length2))
                        break main;
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
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
     * @param x1 first bitmap
     * @param x2 other bitmap
     * @return result of the operation
     */
    public static RoaringBitmap andNot(final ImmutableRoaringBitmap x1,
                                       final ImmutableRoaringBitmap x2) {
        final RoaringBitmap answer = new RoaringBitmap();
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer
                .size();
        main:
        if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            do {
                if (s1 < s2) {
                    answer.highLowContainer.appendCopy(x1.highLowContainer, pos1);
                    pos1++;
                    if (pos1 == length1)
                        break main;
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final Container c = x1.highLowContainer.getContainerAtIndex(pos1).andNot(
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0)
                        answer.highLowContainer.append(s1, c);
                    pos1++;
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2))
                        break main;
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            } while (true);
        }
        if (pos2 == length2) {
            answer.highLowContainer.appendCopy(x1.highLowContainer,
                    pos1, length1);
        }
        return answer;
    }

    /**
     * Complements the bits in the given range, from rangeStart (inclusive)
     * rangeEnd (exclusive). The given bitmap is unchanged.
     *
     * @param bm         bitmap being negated
     * @param rangeStart inclusive beginning of range
     * @param rangeEnd   exclusive ending of range
     * @return a new Bitmap
     */
    public static RoaringBitmap flip(ImmutableRoaringBitmap bm,
                                     final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd) {
            throw new RuntimeException("Invalid range " + rangeStart + " -- " + rangeEnd);
        }

        RoaringBitmap answer = new RoaringBitmap();
        final short hbStart = Util.highbits(rangeStart);
        final short lbStart = Util.lowbits(rangeStart);
        final short hbLast = Util.highbits(rangeEnd - 1);
        final short lbLast = Util.lowbits(rangeEnd - 1);

        // copy the containers before the active area
        answer.highLowContainer.appendCopiesUntil(bm.highLowContainer, hbStart);

        final int max = Util.toIntUnsigned(Util.maxLowBit());
        for (short hb = hbStart; hb <= hbLast; ++hb) {
            final int containerStart = (hb == hbStart) ? Util.toIntUnsigned(lbStart) : 0;
            final int containerLast = (hb == hbLast) ? Util.toIntUnsigned(lbLast) : max;

            final int i = bm.highLowContainer.getIndex(hb);
            final int j = answer.highLowContainer.getIndex(hb);
            assert j < 0;

            if (i >= 0) {
                final Container c = bm.highLowContainer.getContainerAtIndex(i).not(containerStart, containerLast);
                if (c.getCardinality() > 0)
                    answer.highLowContainer.insertNewKeyValueAt(-j - 1, hb, c);

            } else { // *think* the range of ones must never be
                // empty.
                answer.highLowContainer.insertNewKeyValueAt(-j - 1, hb,
                        Container.rangeOfOnes(containerStart, containerLast));
            }
        }
        // copy the containers after the active area.
        answer.highLowContainer.appendCopiesAfter(bm.highLowContainer, hbLast);

        return answer;
    }


    /**
     * Bitwise OR (union) operation. The provided bitmaps are *not*
     * modified. This operation is thread-safe as long as the provided
     * bitmaps remain unchanged.
     * <p/>
     * If you have more than 2 bitmaps, consider using the
     * FastAggregation class.
     *
     * @param x1 first bitmap
     * @param x2 other bitmap
     * @return result of the operation
     */
    public static RoaringBitmap or(final ImmutableRoaringBitmap x1,
                                   final ImmutableRoaringBitmap x2) {
        final RoaringBitmap answer = new RoaringBitmap();
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
        main:
        if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            while (true) {
                if (s1 < s2) {
                    answer.highLowContainer.appendCopy(x1.highLowContainer, pos1);
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    answer.highLowContainer.appendCopy(x2.highLowContainer, pos2);
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    answer.highLowContainer.append(s1,
                                    x1.highLowContainer.getContainerAtIndex(pos1).or(
                                            x2.highLowContainer.getContainerAtIndex(pos2))
                            );
                    pos1++;
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2)) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            }
        }
        if (pos1 == length1) {
            answer.highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
        } else if (pos2 == length2) {
            answer.highLowContainer.appendCopy(x1.highLowContainer, pos1, length1);
        }
        return answer;
    }

    /**
     * Bitwise XOR (symmetric difference) operation. The provided bitmaps
     * are *not* modified. This operation is thread-safe as long as the
     * provided bitmaps remain unchanged.
     * <p/>
     * If you have more than 2 bitmaps, consider using the
     * FastAggregation class.
     *
     * @param x1 first bitmap
     * @param x2 other bitmap
     * @return result of the operation
     */
    public static RoaringBitmap xor(final ImmutableRoaringBitmap x1,
                                    final ImmutableRoaringBitmap x2) {
        final RoaringBitmap answer = new RoaringBitmap();
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

        main:
        if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            while (true) {
                if (s1 < s2) {
                    answer.highLowContainer.appendCopy(x1.highLowContainer, pos1);
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    answer.highLowContainer.appendCopy(x2.highLowContainer, pos2);
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final Container c = x1.highLowContainer.getContainerAtIndex(pos1).xor(
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0)
                        answer.highLowContainer.append(s1, c);
                    pos1++;
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2)) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            }
        }
        if (pos1 == length1) {
            answer.highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
        } else if (pos2 == length2) {
            answer.highLowContainer.appendCopy(x1.highLowContainer, pos1, length1);
        }

        return answer;
    }

    protected RoaringArray highLowContainer = null;

    protected ImmutableRoaringBitmap() {

    }

    /**
     * Constructs a new ImmutableRoaringBitmap. Only meta-data is loaded to
     * RAM. The rest is mapped to the ByteBuffer.
     *
     * @param b data source
     */
    public ImmutableRoaringBitmap(final ByteBuffer b) {
        highLowContainer = new RoaringArray(b);
    }

    /**
     * Checks whether the value in included, which is equivalent to checking
     * if the corresponding bit is set (get in BitSet class).
     *
     * @param x integer value
     * @return whether the integer value is included.
     */
    public boolean contains(final int x) {
        final short hb = Util.highbits(x);
        final Container c = highLowContainer.getContainer(hb);
        return c != null && c.contains(Util.lowbits(x));
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ImmutableRoaringBitmap) {
            final ImmutableRoaringBitmap srb = (ImmutableRoaringBitmap) o;
            return srb.highLowContainer.equals(this.highLowContainer);
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
        for (int i = 0; i < this.highLowContainer.size(); i++) {
            size += this.highLowContainer.getContainerAtIndex(i).getCardinality();
        }
        return size;
    }

    private IntIterator getIntIterator() {
        return new IntIterator() {
            int hs = 0;

            Iterator<Short> iter;

            short pos = 0;

            int x;

            @Override
            public boolean hasNext() {
                return pos < ImmutableRoaringBitmap.this.highLowContainer.size();
            }

            public IntIterator init() {
                if (pos < ImmutableRoaringBitmap.this.highLowContainer.size()) {
                    iter = ImmutableRoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).iterator();
                    hs = Util.toIntUnsigned(ImmutableRoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
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

        }.init();
    }

    /**
     * Estimate of the memory usage of this data structure. This
     * can be expected to be within 1% of the true memory usage.
     * If exact measures are needed, we recommend using dedicated
     * libraries such as SizeOf.
     * <p/>
     * When the bitmap is constructed from a ByteBuffer from a
     * memory-mapped file, this estimate is invalid: we can expect
     * the actual memory usage to be significantly (e.g., 10x) less.
     *
     * @return estimated memory usage.
     */
    public int getSizeInBytes() {
        int size = 2;
        for (int i = 0; i < this.highLowContainer.size(); i++) {
            final Container c = this.highLowContainer.getContainerAtIndex(i);
            size += 4 + c.getSizeInBytes();
        }
        return size;
    }

    @Override
    public int hashCode() {
        return highLowContainer.hashCode();
    }

    /**
     * iterate over the positions of the true values.
     *
     * @return the iterator
     */
    @Override
    public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            int hs = 0;

            Iterator<Short> iter;

            short pos = 0;

            int x;

            @Override
            public boolean hasNext() {
                return pos < ImmutableRoaringBitmap.this.highLowContainer.size();
            }

            public Iterator<Integer> init() {
                if (pos < ImmutableRoaringBitmap.this.highLowContainer.size()) {
                    iter = ImmutableRoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).iterator();
                    hs = Util.toIntUnsigned(ImmutableRoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
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

        }.init();
    }

    /**
     * Serialize the bitmap. You can later reconstruct
     * the bitmap with RoaringBitmap.deserialize.
     *
     * @param out the DataOutput stream
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void serialize(DataOutput out) throws IOException {
        this.highLowContainer.serialize(out);

    }

    /**
     * Report the number of bytes required for serialization.
     * This count will match the bytes written when calling
     * the serialize method. The writeExternal method will
     * use slightly more space due to its serialization overhead.
     *
     * @return the size in bytes
     */
    public int serializedSizeInBytes() {
        return highLowContainer.serializedSizeInBytes();
    }

    /**
     * Return the set values as an array.
     *
     * @return array representing the set values.
     */
    public int[] toArray() {
        final int[] array = new int[this.getCardinality()];
        int pos = 0, pos2 = 0;
        while (pos < this.highLowContainer.size()) {
            final int hs = Util.toIntUnsigned(this.highLowContainer.getKeyAtIndex(pos)) << 16;
            final Container c = this.highLowContainer.getContainerAtIndex(pos++);
            c.fillLeastSignificant16bits(array, pos2, hs);
            pos2 += c.getCardinality();
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
        final StringBuilder answer = new StringBuilder();
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
     * @param out output stream
     * @throws IOException
     */
    public void writeExternal(final ObjectOutput out) throws IOException {
        this.highLowContainer.writeExternal(out);
    }
}