/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;

import org.roaringbitmap.IntIterator;

import java.io.*;
import java.util.Iterator;

/**
 * MutableRoaringBitmap, a compressed alternative to the BitSet.
 * It is similar to org.roaringbitmap.RoaringBitmap, but it differs in that it
 * can interact with ImmutableRoaringBitmap objects.
 */
public final class MutableRoaringBitmap extends ImmutableRoaringBitmap
        implements Cloneable, Serializable, Iterable<Integer>, Externalizable {
    private static final long serialVersionUID = 3L;

    /**
     * Bitwise AND (intersection) operation. The provided bitmaps are *not*
     * modified. This operation is thread-safe as long as the provided bitmaps
     * remain unchanged.
     * 
     * @param x1
     *            first bitmap
     * @param x2
     *            other bitmap
     * @return result of the operation
     */
    public static MutableRoaringBitmap and(final MutableRoaringBitmap x1,
            final MutableRoaringBitmap x2) {
        final MutableRoaringBitmap answer = new MutableRoaringBitmap();

        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer
                .size();
        /*
         * TODO: This could be optimized quite a bit when one bitmap is much
         * smaller than the other one.
         */
        main: if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            do {
                if (s1 < s2) {
                    pos1++;
                    if (pos1 == length1)
                        break main;
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    pos2++;
                    if (pos2 == length2)
                        break main;
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final MappeableContainer c = x1.highLowContainer
                            .getContainerAtIndex(pos1).and(
                                    x2.highLowContainer
                                            .getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0)
                        answer.getMappeableRoaringArray().append(s1, c);
                    pos1++;
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2))
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
     * modified. This operation is thread-safe as long as the provided bitmaps
     * remain unchanged.
     * 
     * @param x1
     *            first bitmap
     * @param x2
     *            other bitmap
     * @return result of the operation
     */
    public static MutableRoaringBitmap andNot(final MutableRoaringBitmap x1,
            final MutableRoaringBitmap x2) {
        final MutableRoaringBitmap answer = new MutableRoaringBitmap();
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer
                .size();
        main: if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            do {
                if (s1 < s2) {
                    answer.getMappeableRoaringArray().appendCopy(
                            x1.highLowContainer.getKeyAtIndex(pos1),
                            x1.highLowContainer.getContainerAtIndex(pos1));
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
                    final MappeableContainer c = x1.highLowContainer
                            .getContainerAtIndex(pos1).andNot(
                                    x2.highLowContainer
                                            .getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0)
                        answer.getMappeableRoaringArray().append(s1, c);
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
            answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer,
                    pos1, length1);
        }
        return answer;
    }

    /**
     * Generate a bitmap with the specified values set to true. The provided
     * integers values don't have to be in sorted order, but it may be
     * preferable to sort them from a performance point of view.
     * 
     * @param dat
     *            set values
     * @return a new bitmap
     */
    public static MutableRoaringBitmap bitmapOf(final int... dat) {
        final MutableRoaringBitmap ans = new MutableRoaringBitmap();
        for (final int i : dat)
            ans.add(i);
        return ans;
    }

    /**
     * Complements the bits in the given range, from rangeStart (inclusive)
     * rangeEnd (exclusive). The given bitmap is unchanged.
     * 
     * @param bm
     *            bitmap being negated
     * @param rangeStart
     *            inclusive beginning of range
     * @param rangeEnd
     *            exclusive ending of range
     * @return a new Bitmap
     */
    public static MutableRoaringBitmap flip(MutableRoaringBitmap bm,
            final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd) {
            return bm.clone();
        }

        MutableRoaringBitmap answer = new MutableRoaringBitmap();
        final short hbStart = BufferUtil.highbits(rangeStart);
        final short lbStart = BufferUtil.lowbits(rangeStart);
        final short hbLast = BufferUtil.highbits(rangeEnd - 1);
        final short lbLast = BufferUtil.lowbits(rangeEnd - 1);

        // copy the containers before the active area
        answer.getMappeableRoaringArray().appendCopiesUntil(
                bm.highLowContainer, hbStart);

        final int max = BufferUtil.toIntUnsigned(BufferUtil.maxLowBit());
        for (short hb = hbStart; hb <= hbLast; ++hb) {
            final int containerStart = (hb == hbStart) ? BufferUtil
                    .toIntUnsigned(lbStart) : 0;
            final int containerLast = (hb == hbLast) ? BufferUtil
                    .toIntUnsigned(lbLast) : max;

            final int i = bm.highLowContainer.getIndex(hb);
            final int j = answer.highLowContainer.getIndex(hb);
            assert j < 0;

            if (i >= 0) {
                final MappeableContainer c = bm.highLowContainer
                        .getContainerAtIndex(i).not(containerStart,
                                containerLast);
                if (c.getCardinality() > 0)
                    answer.getMappeableRoaringArray().insertNewKeyValueAt(
                            -j - 1, hb, c);

            } else { // *think* the range of ones must never be
                // empty.
                answer.getMappeableRoaringArray().insertNewKeyValueAt(
                        -j - 1,
                        hb,
                        MappeableContainer.rangeOfOnes(containerStart,
                                containerLast));
            }
        }
        // copy the containers after the active area.
        answer.getMappeableRoaringArray().appendCopiesAfter(
                bm.highLowContainer, hbLast);

        return answer;
    }

    /**
     * Bitwise OR (union) operation. The provided bitmaps are *not* modified.
     * This operation is thread-safe as long as the provided bitmaps remain
     * unchanged.
     * 
     * @param x1
     *            first bitmap
     * @param x2
     *            other bitmap
     * @return result of the operation
     */
    public static MutableRoaringBitmap or(final MutableRoaringBitmap x1,
            final MutableRoaringBitmap x2) {
        final MutableRoaringBitmap answer = new MutableRoaringBitmap();
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer
                .size();
        main: if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            while (true) {
                if (s1 < s2) {
                    answer.getMappeableRoaringArray().appendCopy(
                            x1.highLowContainer.getKeyAtIndex(pos1),
                            x1.highLowContainer.getContainerAtIndex(pos1));
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    answer.getMappeableRoaringArray().appendCopy(
                            x2.highLowContainer.getKeyAtIndex(pos2),
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    answer.getMappeableRoaringArray().append(
                            s1,
                            x1.highLowContainer.getContainerAtIndex(pos1).or(
                                    x2.highLowContainer
                                            .getContainerAtIndex(pos2)));
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
            answer.getMappeableRoaringArray().appendCopy(x2.highLowContainer,
                    pos2, length2);
        } else if (pos2 == length2) {
            answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer,
                    pos1, length1);
        }
        return answer;
    }

    /**
     * Bitwise XOR (symmetric difference) operation. The provided bitmaps are
     * *not* modified. This operation is thread-safe as long as the provided
     * bitmaps remain unchanged.
     * 
     * @param x1
     *            first bitmap
     * @param x2
     *            other bitmap
     * @return result of the operation
     */
    public static MutableRoaringBitmap xor(final MutableRoaringBitmap x1,
            final MutableRoaringBitmap x2) {
        final MutableRoaringBitmap answer = new MutableRoaringBitmap();
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer
                .size();

        main: if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            while (true) {
                if (s1 < s2) {
                    answer.getMappeableRoaringArray().appendCopy(
                            x1.highLowContainer.getKeyAtIndex(pos1),
                            x1.highLowContainer.getContainerAtIndex(pos1));
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    answer.getMappeableRoaringArray().appendCopy(
                            x2.highLowContainer.getKeyAtIndex(pos2),
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final MappeableContainer c = x1.highLowContainer
                            .getContainerAtIndex(pos1).xor(
                                    x2.highLowContainer
                                            .getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0)
                        answer.getMappeableRoaringArray().append(s1, c);
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
            answer.getMappeableRoaringArray().appendCopy(x2.highLowContainer,
                    pos2, length2);
        } else if (pos2 == length2) {
            answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer,
                    pos1, length1);
        }

        return answer;
    }

    /**
     * Create an empty bitmap
     */
    public MutableRoaringBitmap() {
        highLowContainer = new MutableRoaringArray();
    }

    /**
     * set the value to "true", whether it already appears or not.
     * 
     * @param x
     *            integer value
     */
    public void add(final int x) {
        final short hb = BufferUtil.highbits(x);
        final int i = highLowContainer.getIndex(hb);
        if (i >= 0) {
            getMappeableRoaringArray().setContainerAtIndex(
                    i,
                    highLowContainer.getContainerAtIndex(i).add(
                            BufferUtil.lowbits(x)));
        } else {
            final MappeableArrayContainer newac = new MappeableArrayContainer();
            getMappeableRoaringArray().insertNewKeyValueAt(-i - 1, hb,
                    newac.add(BufferUtil.lowbits(x)));
        }
    }

    /**
     * In-place bitwise AND (intersection) operation. The current bitmap is
     * modified.
     * 
     * @param array
     *            other bitmap
     */
    public void and(final ImmutableRoaringBitmap array) {
        int pos1 = 0, pos2 = 0;
        int length1 = highLowContainer.size();
        final int length2 = array.highLowContainer.size();
        /*
         * TODO: This could be optimized quite a bit when one bitmap is much
         * smaller than the other one.
         */
        main: if (pos1 < length1 && pos2 < length2) {
            short s1 = highLowContainer.getKeyAtIndex(pos1);
            short s2 = array.highLowContainer.getKeyAtIndex(pos2);
            do {
                if (s1 < s2) {
                    getMappeableRoaringArray().removeAtIndex(pos1);
                    --length1;
                    if (pos1 == length1)
                        break main;
                    s1 = highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    pos2++;
                    if (pos2 == length2)
                        break main;
                    s2 = array.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final MappeableContainer c = highLowContainer
                            .getContainerAtIndex(pos1).iand(
                                    array.highLowContainer
                                            .getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0) {
                        this.getMappeableRoaringArray().setContainerAtIndex(
                                pos1, c);
                        pos1++;
                    } else {
                        getMappeableRoaringArray().removeAtIndex(pos1);
                        --length1;
                    }
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2))
                        break main;
                    s1 = highLowContainer.getKeyAtIndex(pos1);
                    s2 = array.highLowContainer.getKeyAtIndex(pos2);
                }
            } while (true);
        }
        getMappeableRoaringArray().resize(pos1);
    }

    /**
     * In-place bitwise ANDNOT (difference) operation. The current bitmap is
     * modified.
     * 
     * @param x2
     *            other bitmap
     */
    public void andNot(final ImmutableRoaringBitmap x2) {
        int pos1 = 0, pos2 = 0;
        int length1 = highLowContainer.size();
        final int length2 = x2.highLowContainer.size();
        main: if (pos1 < length1 && pos2 < length2) {
            short s1 = highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            do {
                if (s1 < s2) {
                    pos1++;
                    if (pos1 == length1)
                        break main;
                    s1 = highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final MappeableContainer c = highLowContainer
                            .getContainerAtIndex(pos1).iandNot(
                                    x2.highLowContainer
                                            .getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0) {
                        this.getMappeableRoaringArray().setContainerAtIndex(
                                pos1, c);
                        pos1++;
                    } else {
                        getMappeableRoaringArray().removeAtIndex(pos1);
                        --length1;
                    }
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2))
                        break main;
                    s1 = highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            } while (true);
        }
    }

    /**
     * reset to an empty bitmap; result occupies as much space a newly created
     * bitmap.
     */
    public void clear() {
        highLowContainer = new MutableRoaringArray(); // lose references
    }

    @Override
    public MutableRoaringBitmap clone() {
        final MutableRoaringBitmap x = (MutableRoaringBitmap) super.clone();
        x.highLowContainer = highLowContainer.clone();
        return x;

    }

    /**
     * Deserialize the bitmap (retrieve from the input stream). The current
     * bitmap is overwritten.
     * 
     * @param in
     *            the DataInput stream
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void deserialize(DataInput in) throws IOException {
        getMappeableRoaringArray().deserialize(in);
    }

    /**
     * Modifies the current bitmap by complementing the bits in the given range,
     * from rangeStart (inclusive) rangeEnd (exclusive).
     * 
     * @param rangeStart
     *            inclusive beginning of range
     * @param rangeEnd
     *            exclusive ending of range
     */
    public void flip(final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd)
            return; // empty range

        final short hbStart = BufferUtil.highbits(rangeStart);
        final short lbStart = BufferUtil.lowbits(rangeStart);
        final short hbLast = BufferUtil.highbits(rangeEnd - 1);
        final short lbLast = BufferUtil.lowbits(rangeEnd - 1);

        final int max = BufferUtil.toIntUnsigned(BufferUtil.maxLowBit());
        for (short hb = hbStart; hb <= hbLast; ++hb) {
            // first container may contain partial range
            final int containerStart = (hb == hbStart) ? BufferUtil
                    .toIntUnsigned(lbStart) : 0;
            // last container may contain partial range
            final int containerLast = (hb == hbLast) ? BufferUtil
                    .toIntUnsigned(lbLast) : max;
            final int i = highLowContainer.getIndex(hb);

            if (i >= 0) {
                final MappeableContainer c = highLowContainer
                        .getContainerAtIndex(i).inot(containerStart,
                                containerLast);
                if (c.getCardinality() > 0)
                    getMappeableRoaringArray().setContainerAtIndex(i, c);
                else
                    getMappeableRoaringArray().removeAtIndex(i);
            } else {
                getMappeableRoaringArray().insertNewKeyValueAt(
                        -i - 1,
                        hb,
                        MappeableContainer.rangeOfOnes(containerStart,
                                containerLast));
            }
        }
    }

    public IntIterator getIntIterator() {
        return new IntIterator() {
            int hs = 0;

            Iterator<Short> iter;

            short pos = 0;

            int x;

            @Override
            public boolean hasNext() {
                return pos < MutableRoaringBitmap.this.highLowContainer.size();
            }

            public IntIterator init() {
                if (pos < MutableRoaringBitmap.this.highLowContainer.size()) {
                    iter = MutableRoaringBitmap.this.highLowContainer
                            .getContainerAtIndex(pos).iterator();
                    hs = BufferUtil
                            .toIntUnsigned(MutableRoaringBitmap.this.highLowContainer
                                    .getKeyAtIndex(pos)) << 16;
                }
                return this;
            }

            @Override
            public int next() {
                x = BufferUtil.toIntUnsigned(iter.next()) | hs;
                if (!iter.hasNext()) {
                    ++pos;
                    init();
                }
                return x;
            }


        }.init();
    }

    /**
     * @return a mutable copy of this bitmap
     */
    public MutableRoaringArray getMappeableRoaringArray() {
        return (MutableRoaringArray) highLowContainer;
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
                return pos < MutableRoaringBitmap.this.highLowContainer.size();
            }

            public Iterator<Integer> init() {
                if (pos < MutableRoaringBitmap.this.highLowContainer.size()) {
                    iter = MutableRoaringBitmap.this.highLowContainer
                            .getContainerAtIndex(pos).iterator();
                    hs = BufferUtil
                            .toIntUnsigned(MutableRoaringBitmap.this.highLowContainer
                                    .getKeyAtIndex(pos)) << 16;
                }
                return this;
            }

            @Override
            public Integer next() {
                x = BufferUtil.toIntUnsigned(iter.next()) | hs;
                if (!iter.hasNext()) {
                    ++pos;
                    init();
                }
                return x;
            }

            @Override
            public void remove() {
                if ((x & hs) == hs) {// still in same container
                    iter.remove();
                } else {
                    MutableRoaringBitmap.this.remove(x);
                }
            }

        }.init();
    }

    /**
     * In-place bitwise OR (union) operation. The current bitmap is modified.
     * 
     * @param x2
     *            other bitmap
     */
    public void or(final ImmutableRoaringBitmap x2) {
        int pos1 = 0, pos2 = 0;
        int length1 = highLowContainer.size();
        final int length2 = x2.highLowContainer.size();
        main: if (pos1 < length1 && pos2 < length2) {
            short s1 = highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            while (true) {
                if (s1 < s2) {
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    getMappeableRoaringArray().insertNewKeyValueAt(pos1, s2,
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    pos1++;
                    length1++;
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    getMappeableRoaringArray().setContainerAtIndex(
                            pos1,
                            highLowContainer.getContainerAtIndex(pos1).ior(
                                    x2.highLowContainer
                                            .getContainerAtIndex(pos2)));
                    pos1++;
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2)) {
                        break main;
                    }
                    s1 = highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            }
        }
        if (pos1 == length1) {
            getMappeableRoaringArray().appendCopy(x2.highLowContainer, pos2,
                    length2);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        getMappeableRoaringArray().readExternal(in);

    }

    /**
     * If present remove the specified integers (effectively, sets its bit value
     * to false)
     * 
     * @param x
     *            integer value representing the index in a bitmap
     */
    public void remove(final int x) {
        final short hb = BufferUtil.highbits(x);
        final int i = highLowContainer.getIndex(hb);
        if (i < 0)
            return;
        getMappeableRoaringArray().setContainerAtIndex(
                i,
                highLowContainer.getContainerAtIndex(i).remove(
                        BufferUtil.lowbits(x)));
        if (highLowContainer.getContainerAtIndex(i).getCardinality() == 0)
            getMappeableRoaringArray().removeAtIndex(i);
    }

    /**
     * Serialize this bitmap.
     * 
     * The current bitmap is not modified.
     * 
     * @param out
     *            the DataOutput stream
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void serialize(DataOutput out) throws IOException {
        getMappeableRoaringArray().serialize(out);
    }

    /**
     * Report the number of bytes required for serialization. This count will
     * match the bytes written when calling the serialize method. The
     * writeExternal method will use slightly more space due to its
     * serialization overhead.
     * 
     * @return the size in bytes
     */
    public int serializedSizeInBytes() {
        return this.getMappeableRoaringArray().serializedSizeInBytes();
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
     * Recover allocated but unused memory.
     */
    public void trim() {
        for (int i = 0; i < this.highLowContainer.size(); i++) {
            this.highLowContainer.getContainerAtIndex(i).trim();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        getMappeableRoaringArray().writeExternal(out);
    }

    /**
     * In-place bitwise XOR (symmetric difference) operation. The current bitmap
     * is modified.
     * 
     * @param x2
     *            other bitmap
     */
    public void xor(final ImmutableRoaringBitmap x2) {
        int pos1 = 0, pos2 = 0;
        int length1 = highLowContainer.size();
        final int length2 = x2.highLowContainer.size();

        main: if (pos1 < length1 && pos2 < length2) {
            short s1 = highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            while (true) {
                if (s1 < s2) {
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    getMappeableRoaringArray().insertNewKeyValueAt(pos1, s2,
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    pos1++;
                    length1++;
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final MappeableContainer c = highLowContainer
                            .getContainerAtIndex(pos1).ixor(
                                    x2.highLowContainer
                                            .getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0) {
                        this.getMappeableRoaringArray().setContainerAtIndex(
                                pos1, c);
                        pos1++;
                    } else {
                        getMappeableRoaringArray().removeAtIndex(pos1);
                        --length1;
                    }
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2)) {
                        break main;
                    }
                    s1 = highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            }
        }
        if (pos1 == length1) {
            getMappeableRoaringArray().appendCopy(x2.highLowContainer, pos2,
                    length2);
        }
    }

}
