/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.io.*;
import java.util.Iterator;

/**
 * RoaringBitmap, a compressed alternative to the BitSet.
 */
public final class RoaringBitmap implements Cloneable, Serializable, Iterable<Integer>, Externalizable {

    private static final long serialVersionUID = 6L;

    /**
     * Bitwise AND (intersection) operation. The provided bitmaps are *not*
     * modified. This operation is thread-safe as long as the provided
     * bitmaps remain unchanged.
     * 
     * If you have more than 2 bitmaps, consider using the
     * FastAggregation class.
     *
     * @param x1 first bitmap
     * @param x2 other bitmap
     * @return result of the operation
     * @see FastAggregation#and(RoaringBitmap...)
     */
    public static RoaringBitmap and(final RoaringBitmap x1,
                                    final RoaringBitmap x2) {
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
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    pos2++;
                    if (pos2 == length2)
                        break main;
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final Container c = x1.highLowContainer
                            .getContainerAtIndex(pos1)
                            .and(x2.highLowContainer.getContainerAtIndex(pos2));
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
    public static RoaringBitmap andNot(final RoaringBitmap x1,
                                       final RoaringBitmap x2) {
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
                    answer.highLowContainer.appendCopy(
                            x1.highLowContainer, pos1);
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
                    final Container c = x1.highLowContainer
                            .getContainerAtIndex(pos1)
                            .andNot(x2.highLowContainer.getContainerAtIndex(pos2));
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
            answer.highLowContainer.appendCopy(x1.highLowContainer, pos1, length1);
        }
        return answer;
    }

    /**
     * Generate a bitmap with the specified values set to true. The provided
     * integers values don't have to be in sorted order, but it may be
     * preferable to sort them from a performance point of view.
     *
     * @param dat set values
     * @return a new bitmap
     */
    public static RoaringBitmap bitmapOf(final int... dat) {
        final RoaringBitmap ans = new RoaringBitmap();
        for (final int i : dat)
            ans.add(i);
        return ans;
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
    public static RoaringBitmap flip(RoaringBitmap bm,final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd) {
            return bm.clone();
        }

        RoaringBitmap answer = new RoaringBitmap();
        final short hbStart = Util.highbits(rangeStart);
        final short lbStart = Util.lowbits(rangeStart);
        final short hbLast = Util.highbits(rangeEnd - 1);
        final short lbLast = Util.lowbits(rangeEnd - 1);

        // copy the containers before the active area
        answer.highLowContainer.appendCopiesUntil(bm.highLowContainer, hbStart);

        int max = Util.toIntUnsigned(Util.maxLowBit());
        for (short hb = hbStart; hb <= hbLast; ++hb) {
            final int containerStart = (hb == hbStart) ? Util.toIntUnsigned(lbStart) : 0;
            final int containerLast = (hb == hbLast) ? Util.toIntUnsigned(lbLast) : max;

            final int i = bm.highLowContainer.getIndex(hb);
            final int j = answer.highLowContainer.getIndex(hb);
            assert j < 0;

            if (i >= 0) {
                Container c = bm.highLowContainer.getContainerAtIndex(i).not(containerStart, containerLast);
                if (c.getCardinality() > 0)
                    answer.highLowContainer.insertNewKeyValueAt(-j - 1, hb, c);

            } else { // *think* the range of ones must never be
                // empty.
                answer.highLowContainer.insertNewKeyValueAt(-j - 1, hb, Container.rangeOfOnes(
                                containerStart, containerLast)
                );
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
     * 
     * If you have more than 2 bitmaps, consider using the
     * FastAggregation class.
     *
     * @param x1 first bitmap
     * @param x2 other bitmap
     * @return result of the operation
     * @see FastAggregation#or(RoaringBitmap...)
     * @see FastAggregation#horizontal_or(RoaringBitmap...)
     */
    public static RoaringBitmap or(final RoaringBitmap x1,
                                   final RoaringBitmap x2) {
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
                    answer.highLowContainer.append(s1,x1.highLowContainer.getContainerAtIndex(pos1).or(
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
     * 
     * If you have more than 2 bitmaps, consider using the
     * FastAggregation class.
     *
     * @param x1 first bitmap
     * @param x2 other bitmap
     * @return result of the operation
     * @see FastAggregation#xor(RoaringBitmap...)
     * @see FastAggregation#horizontal_xor(RoaringBitmap...)
     */
    public static RoaringBitmap xor(final RoaringBitmap x1, final RoaringBitmap x2) {
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
                    if ((pos1 == length1)
                            || (pos2 == length2)) {
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

    RoaringArray highLowContainer = null;

    /**
     * Create an empty bitmap
     */
    public RoaringBitmap() {
        highLowContainer = new RoaringArray();
    }

    /**
     * set the value to "true", whether it already appears or not.
     *
     * @param x integer value
     */
    public void add(final int x) {
        final short hb = Util.highbits(x);
        final int i = highLowContainer.getIndex(hb);
        if (i >= 0) {
            highLowContainer.setContainerAtIndex(i,
                    highLowContainer.getContainerAtIndex(i).add(Util.lowbits(x))
            );
        } else {
            final ArrayContainer newac = new ArrayContainer();
            highLowContainer.insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
        }
    }

    /**
     * In-place bitwise AND (intersection) operation. The current bitmap is
     * modified.
     *
     * @param x2 other bitmap
     */
    public void and(final RoaringBitmap x2) {
        int pos1 = 0, pos2 = 0;
        int length1 = highLowContainer.size();
        final int length2 = x2.highLowContainer.size();
                /*
                 * TODO: This could be optimized quite a bit when one bitmap is
                 * much smaller than the other one.
                 */
        main:
        if (pos1 < length1 && pos2 < length2) {
            short s1 = highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            do {
                if (s1 < s2) {
                    highLowContainer.removeAtIndex(pos1);
                    --length1;
                    if (pos1 == length1)
                        break main;
                    s1 = highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    pos2++;
                    if (pos2 == length2)
                        break main;
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final Container c = highLowContainer.getContainerAtIndex(pos1).iand(
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0) {
                        this.highLowContainer.setContainerAtIndex(pos1, c);
                        pos1++;
                    } else {
                        highLowContainer.removeAtIndex(pos1);
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
        highLowContainer.resize(pos1);
    }

    /**
     * In-place bitwise ANDNOT (difference) operation. The current bitmap is
     * modified.
     *
     * @param x2 other bitmap
     */
    public void andNot(final RoaringBitmap x2) {
        int pos1 = 0, pos2 = 0;
        int length1 = highLowContainer.size();
        final int length2 = x2.highLowContainer.size();
        main:
        if (pos1 < length1 && pos2 < length2) {
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
                    final Container c = highLowContainer.getContainerAtIndex(pos1).iandNot(
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0) {
                        this.highLowContainer.setContainerAtIndex(pos1, c);
                        pos1++;
                    } else {
                        highLowContainer.removeAtIndex(pos1);
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
     * reset to an empty bitmap; result occupies as much space a newly
     * created bitmap.
     */
    public void clear() {
        highLowContainer = new RoaringArray(); // lose references
    }

    @Override
    public RoaringBitmap clone() {
        try {
            final RoaringBitmap x = (RoaringBitmap) super.clone();
            x.highLowContainer = highLowContainer.clone();
            return x;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException("shouldn't happen with clone", e);
        }
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

    /**
     * Deserialize (retrieve) this bitmap.
     * 
     * The current bitmap is overwritten.
     *
     * @param in the DataInput stream
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void deserialize(DataInput in) throws IOException {
        this.highLowContainer.deserialize(in);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RoaringBitmap) {
            final RoaringBitmap srb = (RoaringBitmap) o;
            return srb.highLowContainer.equals(this.highLowContainer);
        }
        return false;
    }


    /**
     * Modifies the current bitmap by complementing the bits in the given
     * range, from rangeStart (inclusive) rangeEnd (exclusive).
     *
     * @param rangeStart inclusive beginning of range
     * @param rangeEnd   exclusive ending of range
     */
    public void flip(final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd)
            return; // empty range

        final short hbStart = Util.highbits(rangeStart);
        final short lbStart = Util.lowbits(rangeStart);
        final short hbLast = Util.highbits(rangeEnd - 1);
        final short lbLast = Util.lowbits(rangeEnd - 1);

        final int max = Util.toIntUnsigned(Util.maxLowBit());
        for (short hb = hbStart; hb <= hbLast; ++hb) {
            // first container may contain partial range
            final int containerStart = (hb == hbStart) ? Util.toIntUnsigned(lbStart) : 0;
            // last container may contain partial range
            final int containerLast = (hb == hbLast) ? Util.toIntUnsigned(lbLast) : max;
            final int i = highLowContainer.getIndex(hb);

            if (i >= 0) {
                final Container c = highLowContainer.getContainerAtIndex(i).inot(
                                containerStart, containerLast);
                if (c.getCardinality() > 0)
                    highLowContainer.setContainerAtIndex(i, c);
                else
                    highLowContainer.removeAtIndex(i);
            } else {
                highLowContainer.insertNewKeyValueAt(-i - 1,hb, Container.rangeOfOnes(
                        containerStart, containerLast)
                );
            }
        }
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

    /**
     * @return a custom iterator over set bits
     */
    public IntIterator getIntIterator() {
        return new IntIterator() {
            int hs = 0;

            Iterator<Short> iter;

            short pos = 0;

            int x;

            @Override
            public boolean hasNext() {
                return pos < RoaringBitmap.this.highLowContainer.size();
            }

            public IntIterator init() {
                if (pos < RoaringBitmap.this.highLowContainer.size()) {
                    iter = RoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).iterator();
                    hs = Util.toIntUnsigned(RoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
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

        }.init();
    }

    /**
     * Estimate of the memory usage of this data structure. This
     * can be expected to be within 1% of the true memory usage.
     *
     * @return estimated memory usage.
     */
    public int getSizeInBytes() {
        int size = 8;
        for (int i = 0; i < this.highLowContainer.size(); i++) {
            final Container c = this.highLowContainer.getContainerAtIndex(i);
            size += 2 + c.getSizeInBytes();
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
                return pos < RoaringBitmap.this.highLowContainer.size();
            }

            public Iterator<Integer> init() {
                if (pos < RoaringBitmap.this.highLowContainer.size()) {
                    iter = RoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).iterator();
                    hs = Util.toIntUnsigned(RoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
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
                if ((x & hs) == hs) {// still in same container
                    iter.remove();
                } else {
                    RoaringBitmap.this.remove(x);
                }
            }

        }.init();
    }

    /**
     * Checks whether the bitmap is empty.
     * 
     * @return true if this bitmap contains no set bit
     */
    public boolean isEmpty() {
    	return highLowContainer.size() == 0;
    }


    /**
     * In-place bitwise OR (union) operation. The current bitmap is
     * modified.
     *
     * @param x2 other bitmap
     */
    public void or(final RoaringBitmap x2) {
        int pos1 = 0, pos2 = 0;
        int length1 = highLowContainer.size();
        final int length2 = x2.highLowContainer.size();
        main:
        if (pos1 < length1 && pos2 < length2) {
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
                    highLowContainer.insertNewKeyValueAt(pos1, s2, x2.highLowContainer.getContainerAtIndex(pos2)
                    );
                    pos1++;
                    length1++;
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    this.highLowContainer.setContainerAtIndex(pos1, highLowContainer.getContainerAtIndex(
                                    pos1).ior(x2.highLowContainer.getContainerAtIndex(pos2))
                    );
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
            highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.highLowContainer.readExternal(in);
    }

    /**
     * If present remove the specified integers (effectively, sets its bit
     * value to false)
     *
     * @param x integer value representing the index in a bitmap
     */
    public void remove(final int x) {
        final short hb = Util.highbits(x);
        final int i = highLowContainer.getIndex(hb);
        if (i < 0)
            return;
        highLowContainer.setContainerAtIndex(i, highLowContainer.getContainerAtIndex(i).remove(Util.lowbits(x)));
        if (highLowContainer.getContainerAtIndex(i).getCardinality() == 0)
            highLowContainer.removeAtIndex(i);
    }

    /**
     * Serialize this bitmap.
     * 
     * The current bitmap is not modified.
     *
     * @param out the DataOutput stream
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void serialize(DataOutput out) throws IOException {
        this.highLowContainer.serialize(out);
    }

    /**
     * Report the number of bytes required to serialize this bitmap.
     * This is the number of bytes written out when using the serialize
     * method. When using the writeExternal method, the count will be
     * higher due to the overhead of Java serialization.
     *
     * @return the size in bytes
     */
    public int serializedSizeInBytes() {
        return this.highLowContainer.serializedSizeInBytes();
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
            Container c = this.highLowContainer.getContainerAtIndex(pos++);
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
     * Recover allocated but unused memory.
     */
    public void trim() {
        for (int i = 0; i < this.highLowContainer.size(); i++) {
            this.highLowContainer.getContainerAtIndex(i).trim();
        }
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        this.highLowContainer.writeExternal(out);
    }

    /**
     * In-place bitwise XOR (symmetric difference) operation. The current
     * bitmap is modified.
     *
     * @param x2 other bitmap
     */
    public void xor(final RoaringBitmap x2) {
        int pos1 = 0, pos2 = 0;
        int length1 = highLowContainer.size();
        final int length2 = x2.highLowContainer.size();

        main:
        if (pos1 < length1 && pos2 < length2) {
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
                    highLowContainer.insertNewKeyValueAt(pos1, s2, x2.highLowContainer.getContainerAtIndex(pos2));
                    pos1++;
                    length1++;
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final Container c = highLowContainer.getContainerAtIndex(pos1).ixor(
                            x2.highLowContainer.getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0) {
                        this.highLowContainer.setContainerAtIndex(pos1, c);
                        pos1++;
                    } else {
                        highLowContainer.removeAtIndex(pos1);
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
            highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
        }
    }
}
