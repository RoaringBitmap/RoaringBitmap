/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.ShortIterator;
import org.roaringbitmap.Util;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * ImmutableRoaringBitmap provides a compressed immutable (cannot be modified)
 * bitmap. It is meant to be used with org.roaringbitmap.buffer.MutableRoaringBitmap.
 * 
 * <pre>
 * {@code
 *       import org.roaringbitmap.buffer.*;
 *       
 *       //...
 *       
 *       MutableRoaringBitmap rr1 = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
 *       MutableRoaringBitmap rr2 = MutableRoaringBitmap.bitmapOf( 2, 3, 1010);
 *       ByteArrayOutputStream bos = new ByteArrayOutputStream();
 *       DataOutputStream dos = new DataOutputStream(bos);
 *       // could call "rr1.runOptimize()" and "rr2.runOptimize" if there 
 *       // there were runs to compress
 *       rr1.serialize(dos);
 *       rr2.serialize(dos);
 *       dos.close();
 *       ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
 *       ImmutableRoaringBitmap rrback1 = new ImmutableRoaringBitmap(bb);
 *       bb.position(bb.position() + rrback1.serializedSizeInBytes());
 *       ImmutableRoaringBitmap rrback2 = new ImmutableRoaringBitmap(bb);
 * }
 * </pre>
 * 
 * It can also be constructed from a ByteBuffer (useful for memory mapping).
 * 
 * Objects of this class may reside almost entirely in memory-map files.
 */
public class ImmutableRoaringBitmap implements Iterable<Integer>, Cloneable, ImmutableBitmapDataProvider {

    /**
     * Bitwise AND (intersection) operation. The provided bitmaps are *not*
     * modified. This operation is thread-safe as long as the provided bitmaps
     * remain unchanged.
     * 
     * If you have more than 2 bitmaps, consider using the FastAggregation
     * class.
     * 
     * @param x1
     *            first bitmap
     * @param x2
     *            other bitmap
     * @return result of the operation
     * @see BufferFastAggregation#and(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap and(final ImmutableRoaringBitmap x1,
                                           final ImmutableRoaringBitmap x2) {
        final MutableRoaringBitmap answer = new MutableRoaringBitmap();
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

        while (pos1 < length1 && pos2 < length2) {
            final short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            final short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            if (s1 == s2) {
                final MappeableContainer c1 = x1.highLowContainer.getContainerAtIndex(pos1);
                final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                final MappeableContainer c = c1.and(c2);
                if (c.getCardinality() > 0) {
                    answer.getMappeableRoaringArray().append(s1, c);
                }
                ++pos1;
                ++pos2;
            } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
                pos1 = x1.highLowContainer.advanceUntil(s2,pos1);
            } else { // s1 > s2
                pos2 = x2.highLowContainer.advanceUntil(s1,pos2);
            }
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
    public static MutableRoaringBitmap andNot(final ImmutableRoaringBitmap x1,
                                              final ImmutableRoaringBitmap x2) {
        final MutableRoaringBitmap answer = new MutableRoaringBitmap();
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

        while (pos1 < length1 && pos2 < length2) {
            final short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            final short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                final MappeableContainer c1 = x1.highLowContainer.getContainerAtIndex(pos1);
                final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                final MappeableContainer c = c1.andNot(c2);
                if (c.getCardinality() > 0) {
                    answer.getMappeableRoaringArray().append(s1, c);
                }
                ++pos1;
                ++pos2;
            } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
                final int nextPos1 = x1.highLowContainer.advanceUntil(s2,pos1);
                answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer, pos1, nextPos1);
                pos1 = nextPos1;
            } else { // s1 > s2
                pos2 = x2.highLowContainer.advanceUntil(s1,pos2);
            }
        }
        if (pos2 == length2) {
            answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer, pos1, length1);
        }
        return answer;
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
    public static MutableRoaringBitmap flip(ImmutableRoaringBitmap bm,
            final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd) {
            throw new RuntimeException("Invalid range " + rangeStart + " -- "
                    + rangeEnd);
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
            final int j = answer.getMappeableRoaringArray().getIndex(hb);
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
                                containerLast+1));
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
     * If you have more than 2 bitmaps, consider using the FastAggregation
     * class.
     * 
     * @param x1
     *            first bitmap
     * @param x2
     *            other bitmap
     * @return result of the operation
     * @see BufferFastAggregation#or(ImmutableRoaringBitmap...)
     * @see BufferFastAggregation#horizontal_or(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap or(final ImmutableRoaringBitmap x1,
            final ImmutableRoaringBitmap x2) {
        final MutableRoaringBitmap answer = new MutableRoaringBitmap();
        MappeableContainerPointer i1 = x1.highLowContainer
                .getContainerPointer();
        MappeableContainerPointer i2 = x2.highLowContainer
                .getContainerPointer();
        main: if (i1.hasContainer() && i2.hasContainer()) {
            while (true) {
                if (i1.key() == i2.key()) {
                    answer.getMappeableRoaringArray().append(i1.key(),
                            i1.getContainer().or(i2.getContainer()));
                    i1.advance();
                    i2.advance();
                    if (!i1.hasContainer() || !i2.hasContainer())
                        break main;
                } else if (Util.compareUnsigned(i1.key(), i2.key()) < 0) { // i1.key() < i2.key()
                    answer.getMappeableRoaringArray().appendCopy(i1.key(),
                            i1.getContainer());
                    i1.advance();
                    if (!i1.hasContainer())
                        break main;
                } else { // i1.key() > i2.key()
                    answer.getMappeableRoaringArray().appendCopy(i2.key(),
                            i2.getContainer());
                    i2.advance();
                    if (!i2.hasContainer())
                        break main;
                }
            }
        }
        if (!i1.hasContainer()) {
            while (i2.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i2.key(),
                        i2.getContainer());
                i2.advance();
            }
        } else if (!i2.hasContainer()) {
            while (i1.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i1.key(),
                        i1.getContainer());
                i1.advance();
            }
        }
        return answer;
    }
    
    // important: inputs should not have been computed lazily
    protected static MutableRoaringBitmap lazyor(final ImmutableRoaringBitmap x1,
            final ImmutableRoaringBitmap x2) {
        final MutableRoaringBitmap answer = new MutableRoaringBitmap();
        MappeableContainerPointer i1 = x1.highLowContainer
                .getContainerPointer();
        MappeableContainerPointer i2 = x2.highLowContainer
                .getContainerPointer();
        main: if (i1.hasContainer() && i2.hasContainer()) {
            while (true) {
                if (i1.key() == i2.key()) {
                    answer.getMappeableRoaringArray().append(i1.key(),
                            i1.getContainer().lazyOR(i2.getContainer()));
                    i1.advance();
                    i2.advance();
                    if (!i1.hasContainer() || !i2.hasContainer())
                        break main;
                } else if (Util.compareUnsigned(i1.key(), i2.key()) < 0) { // i1.key() < i2.key()
                    answer.getMappeableRoaringArray().appendCopy(i1.key(),
                            i1.getContainer());
                    i1.advance();
                    if (!i1.hasContainer())
                        break main;
                } else { // i1.key() > i2.key()
                    answer.getMappeableRoaringArray().appendCopy(i2.key(),
                            i2.getContainer());
                    i2.advance();
                    if (!i2.hasContainer())
                        break main;
                }
            }
        }
        if (!i1.hasContainer()) {
            while (i2.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i2.key(),
                        i2.getContainer());
                i2.advance();
            }
        } else if (!i2.hasContainer()) {
            while (i1.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i1.key(),
                        i1.getContainer());
                i1.advance();
            }
        }
        return answer;
    }

    /**
     * Bitwise XOR (symmetric difference) operation. The provided bitmaps are
     * *not* modified. This operation is thread-safe as long as the provided
     * bitmaps remain unchanged.
     * 
     * If you have more than 2 bitmaps, consider using the FastAggregation
     * class.
     * 
     * @param x1
     *            first bitmap
     * @param x2
     *            other bitmap
     * @return result of the operation
     * @see BufferFastAggregation#xor(ImmutableRoaringBitmap...)
     * @see BufferFastAggregation#horizontal_xor(ImmutableRoaringBitmap...)
     */
    public static MutableRoaringBitmap xor(final ImmutableRoaringBitmap x1,
            final ImmutableRoaringBitmap x2) {
        final MutableRoaringBitmap answer = new MutableRoaringBitmap();
        MappeableContainerPointer i1 = x1.highLowContainer
                .getContainerPointer();
        MappeableContainerPointer i2 = x2.highLowContainer
                .getContainerPointer();
        main: if (i1.hasContainer() && i2.hasContainer()) {
            while (true) {
                if (i1.key() == i2.key()) {
                    final MappeableContainer c = i1.getContainer().xor(
                            i2.getContainer());
                    if (c.getCardinality() > 0)
                        answer.getMappeableRoaringArray().append(i1.key(), c);
                    i1.advance();
                    i2.advance();
                    if (!i1.hasContainer() || !i2.hasContainer())
                        break main;
                } else if (Util.compareUnsigned(i1.key(), i2.key()) < 0) { // i1.key() < i2.key()
                    answer.getMappeableRoaringArray().appendCopy(i1.key(),
                            i1.getContainer());
                    i1.advance();
                    if (!i1.hasContainer())
                        break main;
                } else { // i1.key() < i2.key()
                    answer.getMappeableRoaringArray().appendCopy(i2.key(),
                            i2.getContainer());
                    i2.advance();
                    if (!i2.hasContainer())
                        break main;
                }
            }
        }
        if (!i1.hasContainer()) {
            while (i2.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i2.key(),
                        i2.getContainer());
                i2.advance();
            }
        } else if (!i2.hasContainer()) {
            while (i1.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i1.key(),
                        i1.getContainer());
                i1.advance();
            }
        }

        return answer;
    }

    PointableRoaringArray highLowContainer = null;

    protected ImmutableRoaringBitmap() {

    }

    /**
     * Constructs a new ImmutableRoaringBitmap. Only meta-data is loaded to RAM.
     * The rest is mapped to the ByteBuffer.
     * 
     * It is not necessary that limit() on the input ByteBuffer indicates
     * the end of the serialized data.
     * 
     * After creating this ImmutableRoaringBitmap, you can advance to the rest of
     * the data (if there is more) by setting b.position(b.position() + bitmap.serializedSizeInBytes());
     * 
     * Note that the input ByteBuffer is effectively copied (with the slice operation)
     * so you should expect the provided ByteBuffer to remain unchanged.
     * 
     * 
     * 
     * @param b
     *            data source
     */
    public ImmutableRoaringBitmap(final ByteBuffer b) {
        highLowContainer = new ImmutableRoaringArray(b);
    }

    @Override
    public ImmutableRoaringBitmap clone() {
        try {
            final ImmutableRoaringBitmap x = (MutableRoaringBitmap) super
                    .clone();
            x.highLowContainer = highLowContainer.clone();
            return x;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException("shouldn't happen with clone", e);
        }
    }

    /**
     * Checks whether the value in included, which is equivalent to checking if
     * the corresponding bit is set (get in BitSet class).
     * 
     * @param x
     *            integer value
     * @return whether the integer value is included.
     */
    public boolean contains(final int x) {
        final short hb = BufferUtil.highbits(x);
        final MappeableContainer c = highLowContainer.getContainer(hb);
        return c != null && c.contains(BufferUtil.lowbits(x));
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ImmutableRoaringBitmap) {
            if (this.highLowContainer.size() != ((ImmutableRoaringBitmap) o).highLowContainer
                    .size())
                return false;
            MappeableContainerPointer mp1 = this.highLowContainer
                    .getContainerPointer();
            MappeableContainerPointer mp2 = ((ImmutableRoaringBitmap) o).highLowContainer
                    .getContainerPointer();
            while (mp1.hasContainer()) {
                if (mp1.key() != mp2.key())
                     return false;
                if (mp1.getCardinality() != mp2.getCardinality())
                    return false;
                if (!mp1.getContainer().equals(mp2.getContainer()))
                    return false;
                mp1.advance();
                mp2.advance();
            }
            return true;
        }
        return false;
    }

    /**
     * Returns the number of distinct integers added to the bitmap (e.g., number
     * of bits set).
     * 
     * @return the cardinality
     */
    public int getCardinality() {
        int size = 0;
        for(int i = 0 ; i < this.highLowContainer.size(); ++i ) {
            size += this.highLowContainer.getCardinality(i);
        }
        return size;
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
     * @return a custom iterator over set bits, the bits are traversed
     * in ascending sorted order
     */
    public IntIterator getIntIterator() {
        return new ImmutableRoaringIntIterator();
    }

    /**
     * @return a custom iterator over set bits, the bits are traversed
     * in descending sorted order
     */
    public IntIterator getReverseIntIterator() {
        return new ImmutableRoaringReverseIntIterator();
    }

    /**
     * Estimate of the memory usage of this data structure. This can be expected
     * to be within 1% of the true memory usage. If exact measures are needed,
     * we recommend using dedicated libraries such as SizeOf.
     * 
     * When the bitmap is constructed from a ByteBuffer from a memory-mapped
     * file, this estimate is invalid: we can expect the actual memory usage to
     * be significantly (e.g., 10x) less.
     * 
     * @return estimated memory usage.
     */


    public int getSizeInBytes() {
        int size = 4;
        for(int i = 0 ; i < this.highLowContainer.size(); ++i ) {
            if (this.highLowContainer.getContainerAtIndex(i) instanceof MappeableRunContainer) {
                MappeableRunContainer thisRunContainer = (MappeableRunContainer) this.highLowContainer.getContainerAtIndex(i);
                size += 4 + BufferUtil.getSizeInBytesFromCardinalityEtc(0,thisRunContainer.nbrruns, true);
            }
            else
                size += 4 + BufferUtil.getSizeInBytesFromCardinalityEtc(this.highLowContainer.getCardinality(i),0,false);
        }
        return size;
    }

    @Override
    public int hashCode() {
        return highLowContainer.hashCode();
    }


    /**
     * Compute overall OR between bitmaps.
     * 
     * (Effectively calls {@link BufferFastAggregation#or})
     * 
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap or(ImmutableRoaringBitmap... bitmaps) {
        return BufferFastAggregation.or(bitmaps);
    }
    

    /**
     * Compute overall AND between bitmaps.
     *
     * (Effectively calls {@link BufferFastAggregation#or})
     *
     * @param bitmaps input bitmaps
     * @return aggregated bitmap
     */
    public static MutableRoaringBitmap or(@SuppressWarnings("rawtypes") Iterator bitmaps) {
        return BufferFastAggregation.or(bitmaps);
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

            ShortIterator iter;

            short pos = 0;

            int x;

            @Override
            public boolean hasNext() {
                return pos < ImmutableRoaringBitmap.this.highLowContainer
                        .size();
            }

            public Iterator<Integer> init() {
                if (pos < ImmutableRoaringBitmap.this.highLowContainer.size()) {
                    iter = ImmutableRoaringBitmap.this.highLowContainer
                            .getContainerAtIndex(pos).getShortIterator();
                    hs = BufferUtil
                            .toIntUnsigned(ImmutableRoaringBitmap.this.highLowContainer
                                    .getKeyAtIndex(pos)) << 16;
                }
                return this;
            }

            @Override
            public Integer next() {
                x = iter.nextAsInt() | hs;
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
     * Serialize this bitmap.
     * 
     * Consider calling {@link MutableRoaringBitmap#runOptimize} before serialization to
     * improve compression.
     * 
     * The current bitmap is not modified.
     * 
     * @param out
     *            the DataOutput stream
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void serialize(DataOutput out) throws IOException {
        this.highLowContainer.serialize(out);
    }

    /**
     *  Check whether this bitmap has had its runs compressed.
     * @return whether this bitmap has run compression
     */
    public boolean hasRunCompression() {
        return this.highLowContainer.hasRunCompression();
    }

    /**
     * Report the number of bytes required for serialization. This count will
     * match the bytes written when calling the serialize method. 
     * 
     * @return the size in bytes
     */
    public int serializedSizeInBytes() {
        return this.highLowContainer.serializedSizeInBytes();
    }

    /**
     * Return the set values as an array.  The integer
     * values are in sorted order.
     * 
     * @return array representing the set values.
     */
    public int[] toArray() {
        final int[] array = new int[this.getCardinality()];
        int pos = 0, pos2 = 0;
        while (pos < this.highLowContainer.size()) {
            final int hs = BufferUtil.toIntUnsigned(this.highLowContainer
                    .getKeyAtIndex(pos)) << 16;
            final MappeableContainer c = this.highLowContainer
                    .getContainerAtIndex(pos++);
            c.fillLeastSignificant16bits(array, pos2, hs);
            pos2 += c.getCardinality();
        }
        return array;
    }

    /**
     * Copies the content of this bitmap to a bitmap that can be modified.
     * 
     * @return a mutable bitmap.
     */
    public MutableRoaringBitmap toMutableRoaringBitmap() {
        MutableRoaringBitmap c = new MutableRoaringBitmap();
        MappeableContainerPointer mcp = highLowContainer.getContainerPointer();
        while (mcp.hasContainer()) {
            c.getMappeableRoaringArray().appendCopy(mcp.key(),
                    mcp.getContainer());
            mcp.advance();
        }
        return c;
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
    
    private final class ImmutableRoaringIntIterator implements IntIterator {
        private MappeableContainerPointer cp = ImmutableRoaringBitmap.this.highLowContainer
                .getContainerPointer();

        private int hs = 0;

        private ShortIterator iter;

        private boolean ok;

        public ImmutableRoaringIntIterator() {
            nextContainer();
        }
        @Override
        public boolean hasNext() {
            return ok;
        }

        private void nextContainer() {
            ok = cp.hasContainer();
            if (ok) {
                iter = cp.getContainer().getShortIterator();
                hs = BufferUtil.toIntUnsigned(cp.key()) << 16;
            }
        }
        
        @Override
        public IntIterator clone() {
            try {
                ImmutableRoaringIntIterator x = (ImmutableRoaringIntIterator) super.clone();
                x.iter =  this.iter.clone();
                x.cp = this.cp.clone();
                return x;
            } catch (CloneNotSupportedException e) {
                return null;// will not happen
            }
        }


        @Override
        public int next() {
            int x = iter.nextAsInt() | hs;
            if (!iter.hasNext()) {
                cp.advance();
                nextContainer();
            }
            return x;
        }


    }

    private final class ImmutableRoaringReverseIntIterator implements IntIterator {
        private MappeableContainerPointer cp = ImmutableRoaringBitmap.this.highLowContainer
            .getContainerPointer(ImmutableRoaringBitmap.this.highLowContainer.size() - 1);

        private int hs = 0;

        private ShortIterator iter;

        private boolean ok;

        public ImmutableRoaringReverseIntIterator() {
            nextContainer();
        }

        @Override
        public boolean hasNext() {
            return ok;
        }

        private void nextContainer() {
            ok = cp.hasContainer();
            if (ok) {
                iter = cp.getContainer().getReverseShortIterator();
                hs = BufferUtil.toIntUnsigned(cp.key()) << 16;
            }
        }

        @Override
        public IntIterator clone() {
            try {
                ImmutableRoaringIntIterator x = (ImmutableRoaringIntIterator) super.clone();
                x.iter = this.iter.clone();
                x.cp = this.cp.clone();
                return x;
            } catch (CloneNotSupportedException e) {
                return null;// will not happen
            }
        }


        @Override
        public int next() {
            int x = iter.nextAsInt() | hs;
            if (!iter.hasNext()) {
                cp.previous();
                nextContainer();
            }
            return x;
        }


    }


    /**
     * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be GetCardinality()).
     * @param x upper limit
     *
     * @return the rank
     */
    public int rank(int x) {
        int size = 0;
        short xhigh = BufferUtil.highbits(x);
        for (int i = 0; i < this.highLowContainer.size(); i++) {
            short key =  this.highLowContainer.getKeyAtIndex(i);
            if (Util.compareUnsigned(key, xhigh) < 0)
              size += this.highLowContainer.getCardinality(i);
            else 
                return size + this.highLowContainer.getContainerAtIndex(i).rank(BufferUtil.lowbits(x));
        }
        return size;
    }

    /**
     * Return the jth value stored in this bitmap.
     * 
     * @param j index of the value 
     *
     * @return the value
     */
    public int select(int j) {
        int leftover = j;
        for (int i = 0; i < this.highLowContainer.size(); i++) {
            int thiscard = this.highLowContainer.getCardinality(i);
            if(thiscard > leftover) {
                int keycontrib = this.highLowContainer.getKeyAtIndex(i)<<16;
                MappeableContainer c = this.highLowContainer.getContainerAtIndex(i);
                int lowcontrib = BufferUtil.toIntUnsigned(c.select(leftover));
                return  lowcontrib + keycontrib;
            }
            leftover -= thiscard;
        }
        throw new IllegalArgumentException("select "+j+" when the cardinality is "+this.getCardinality());
    }
    


    /**
     * Create a new Roaring bitmap containing at most maxcardinality integers.
     * 
     * @param maxcardinality maximal cardinality
     * @return a new bitmap with cardinality no more than maxcardinality
     */
    public MutableRoaringBitmap limit(int maxcardinality) {
        MutableRoaringBitmap answer = new MutableRoaringBitmap();
        int currentcardinality = 0;        
        for (int i = 0; (currentcardinality < maxcardinality) && ( i < this.highLowContainer.size()); i++) {
            MappeableContainer c = this.highLowContainer.getContainerAtIndex(i);
            if(c.getCardinality() + currentcardinality <= maxcardinality) {
               ((MutableRoaringArray)answer.highLowContainer).append(this.highLowContainer.getKeyAtIndex(i),c.clone());
               currentcardinality += c.getCardinality();
            }  else {
                int leftover = maxcardinality - currentcardinality;
                MappeableContainer limited = c.limit(leftover);
                ((MutableRoaringArray)answer.highLowContainer).append(this.highLowContainer.getKeyAtIndex(i),limited );
                break;
            }
        }
        return answer;
    }

}
