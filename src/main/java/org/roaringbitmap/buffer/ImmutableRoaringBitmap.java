/*
 * (c) Daniel Lemire, Owen Kaser, Samy Chambi, Jon Alvarado, Rory Graves, Björn Sperber
 * Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.buffer;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * ImmutableRoaringBitmap provides a compressed immutable (cannot be modified)
 * bitmap. It is meant to be used with org.roaringbitmap.buffer.RoaringBitmap.
 * 
 *  It can also be constructed from a ByteBuffer
 * (useful for memory mapping).
 * 
 * Objects of this class may reside almost entirely in memory-map files.
 */
public class ImmutableRoaringBitmap implements Iterable<Integer>, Cloneable {

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
     * @see BufferFastAggregation#and(ImmutableRoaringBitmap...)
     */
    public static MappeableRoaringBitmap and(final ImmutableRoaringBitmap x1,
                                    final ImmutableRoaringBitmap x2) {
        final MappeableRoaringBitmap answer = new MappeableRoaringBitmap();
        MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
        MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();
                /*
                 * TODO: This could be optimized quite a bit when one bitmap is
                 * much smaller than the other one.
                 */
        main:
        if(i1.hasContainer() && i2.hasContainer()) {
        	// TODO: no need to copy the keys
            short s1 = i1.key();
            short s2 = i2.key();
            do {
                if (s1 < s2) {
                    i1.advance();
                    if(! i1.hasContainer())
                        break main;
                    s1 = i1.key();
                } else if (s1 > s2) {
                    i2.advance();
                    if(! i2.hasContainer())
                        break main;
                    s2 = i2.key();
                } else {
                    final MappeableContainer c = i1.getContainer().and(i2.getContainer());
                    if (c.getCardinality() > 0)
                        answer.getMappeableRoaringArray().append(s1, c);
                    i1.advance();
                    i2.advance();
                    if(! i1.hasContainer() || ! i2.hasContainer())
                        break main;
                    s1 = i1.key();
                    s2 = i2.key();
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
    public static MappeableRoaringBitmap andNot(final ImmutableRoaringBitmap x1,
                                       final ImmutableRoaringBitmap x2) {
        final MappeableRoaringBitmap answer = new MappeableRoaringBitmap();
        MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
        MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();

        main:
            if(i1.hasContainer() && i2.hasContainer()) {
            	// TODO: no need to copy the keys
                short s1 = i1.key();
                short s2 = i2.key();

            do {
                if (s1 < s2) {
                    answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
                    i1.advance();
                    if(! i1.hasContainer())
                        break main;
                    s1 = i1.key();

                } else if (s1 > s2) {
                    i2.advance();
                    if(! i2.hasContainer())
                        break main;
                    s2 = i2.key();
                } else {
                    final MappeableContainer c = i1.getContainer().andNot(i2.getContainer());
                    if (c.getCardinality() > 0)
                        answer.getMappeableRoaringArray().append(s1, c);
                    i1.advance();
                    i2.advance();
                    if(! i1.hasContainer() || ! i2.hasContainer())
                        break main;
                    s1 = i1.key();
                    s2 = i2.key();
                }
            } while (true);
        }
        if (!i2.hasContainer()) {
        	while(i1.hasContainer()) {
            answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
            i1.advance();
        	}
        }
        return answer;
    }
    
    public MappeableRoaringBitmap toMappeableRoaringBitmap() {
    	MappeableRoaringBitmap c = new MappeableRoaringBitmap();
    	MappeableContainerPointer mcp = highLowContainer.getContainerPointer();
    	while(mcp.hasContainer()) {
    		c.getMappeableRoaringArray().appendCopy(mcp.key(), mcp.getContainer());
    		
    	}
    	return c;
    }
    
    @Override
    public ImmutableRoaringBitmap clone() {
    	try {
            final ImmutableRoaringBitmap x = (MappeableRoaringBitmap) super.clone();
            x.highLowContainer = highLowContainer.clone();
            return x;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException("shouldn't happen with clone", e);
        }
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
    public static MappeableRoaringBitmap flip(ImmutableRoaringBitmap bm,
                                     final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd) {
            throw new RuntimeException("Invalid range " + rangeStart + " -- " + rangeEnd);
        }

        MappeableRoaringBitmap answer = new MappeableRoaringBitmap();
        final short hbStart = BufferUtil.highbits(rangeStart);
        final short lbStart = BufferUtil.lowbits(rangeStart);
        final short hbLast = BufferUtil.highbits(rangeEnd - 1);
        final short lbLast = BufferUtil.lowbits(rangeEnd - 1);

        // copy the containers before the active area
        answer.getMappeableRoaringArray().appendCopiesUntil(bm.highLowContainer, hbStart);

        final int max = BufferUtil.toIntUnsigned(BufferUtil.maxLowBit());
        for (short hb = hbStart; hb <= hbLast; ++hb) {
            final int containerStart = (hb == hbStart) ? BufferUtil.toIntUnsigned(lbStart) : 0;
            final int containerLast = (hb == hbLast) ? BufferUtil.toIntUnsigned(lbLast) : max;

            final int i = bm.highLowContainer.getIndex(hb);
            final int j = answer.getMappeableRoaringArray().getIndex(hb);
            assert j < 0;

            if (i >= 0) {
                final MappeableContainer c = bm.highLowContainer.getContainerAtIndex(i).not(containerStart, containerLast);
                if (c.getCardinality() > 0)
                    answer.getMappeableRoaringArray().insertNewKeyValueAt(-j - 1, hb, c);

            } else { // *think* the range of ones must never be
                // empty.
                answer.getMappeableRoaringArray().insertNewKeyValueAt(-j - 1, hb,
                        MappeableContainer.rangeOfOnes(containerStart, containerLast));
            }
        }
        // copy the containers after the active area.
        answer.getMappeableRoaringArray().appendCopiesAfter(bm.highLowContainer, hbLast);

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
     * @see BufferFastAggregation#or(RoaringBitmap...)
     * @see BufferFastAggregation#horizontal_or(RoaringBitmap...)
     */
    public static MappeableRoaringBitmap or(final ImmutableRoaringBitmap x1,
                                   final ImmutableRoaringBitmap x2) {
        final MappeableRoaringBitmap answer = new MappeableRoaringBitmap();
        MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
        MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();

        main:
            if(i1.hasContainer() && i2.hasContainer()) {
            	// TODO: no need to copy the keys
                short s1 = i1.key();
                short s2 = i2.key();

            while (true) {
                if (s1 < s2) {
                    answer.getMappeableRoaringArray().appendCopy(i1.key(),i1.getContainer());
                    i1.advance();
                    if(! i1.hasContainer())
                        break main;
                    s1 = i1.key();

                } else if (s1 > s2) {
                    answer.getMappeableRoaringArray().appendCopy(i2.key(),i2.getContainer());
                    i2.advance();
                    if(! i2.hasContainer())
                        break main;
                    s2 = i2.key();

                } else {
                    answer.getMappeableRoaringArray().append(s1,i1.getContainer().or(
                                            i2.getContainer())
                            );
                    i1.advance();
                    i2.advance();
                    if(! i1.hasContainer() || ! i2.hasContainer())
                        break main;
                    s1 = i1.key();
                    s2 = i2.key();
                }
            }
        }
        if (!i1.hasContainer()) {
        	while(i2.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i2.key(), i2.getContainer());
                i2.advance();
            	}
        } else if (!i2.hasContainer()) {
        	while(i1.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
                i1.advance();
            	}
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
     * @see BufferFastAggregation#xor(RoaringBitmap...)
     * @see BufferFastAggregation#horizontal_xor(RoaringBitmap...)
     */
    public static MappeableRoaringBitmap xor(final ImmutableRoaringBitmap x1,
                                    final ImmutableRoaringBitmap x2) {
        final MappeableRoaringBitmap answer = new MappeableRoaringBitmap();
        MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
        MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();


        main:
            if(i1.hasContainer() && i2.hasContainer()) {
            	// TODO: no need to copy the keys
                short s1 = i1.key();
                short s2 = i2.key();

            while (true) {
                if (s1 < s2) {
                    answer.getMappeableRoaringArray().appendCopy(i1.key(),i1.getContainer());
                    i1.advance();
                    if(! i1.hasContainer())
                        break main;
                    s1 = i1.key();

                } else if (s1 > s2) {
                    i2.advance();
                    if(! i2.hasContainer())
                        break main;
                    s2 = i2.key();

                } else {
                    final MappeableContainer c = i1.getContainer().xor(
                            i2.getContainer());
                    if (c.getCardinality() > 0)
                        answer.getMappeableRoaringArray().append(s1, c);
                    i1.advance();
                    i2.advance();
                    if(! i1.hasContainer() || ! i2.hasContainer())
                        break main;
                    s1 = i1.key();
                    s2 = i2.key();
                }
            }
        }
        if (!i1.hasContainer()) {
        	while(i2.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i2.key(), i2.getContainer());
                i2.advance();
            	}
        } else if (!i2.hasContainer()) {
        	while(i1.hasContainer()) {
                answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
                i1.advance();
            	}
        }

        return answer;
    }

    protected PointableArray highLowContainer = null;

    protected ImmutableRoaringBitmap() {

    }

    /**
     * Constructs a new ImmutableRoaringBitmap. Only meta-data is loaded to
     * RAM. The rest is mapped to the ByteBuffer.
     *
     * @param b data source
     */
    public ImmutableRoaringBitmap(final ByteBuffer b) {
    	highLowContainer = new ImmutableRoaringArray(b);
    }

    /**
     * Checks whether the value in included, which is equivalent to checking
     * if the corresponding bit is set (get in BitSet class).
     *
     * @param x integer value
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
    	MappeableContainerPointer cp = this.highLowContainer.getContainerPointer();
        int size = 0;
    	while(cp.hasContainer()) {
    		size += cp.getCardinality();
    		cp.advance();
    	}
        return size;
    }

    private IntIterator getIntIterator() {
        return new IntIterator() {
            int hs = 0;
            
            boolean ok;

            Iterator<Short> iter;

    		MappeableContainerPointer cp = ImmutableRoaringBitmap.this.highLowContainer.getContainerPointer();


            @Override
            public boolean hasNext() {
                return ok;
            }

            public IntIterator init() {
            	ok = cp.hasContainer();
            	if(ok) {
            		iter = cp.getContainer().iterator();
            		hs = BufferUtil.toIntUnsigned(cp.key()) << 16;
            	}
                return this;
            }

            @Override
            public int next() {
                int x = BufferUtil.toIntUnsigned(iter.next()) | hs;
                if (!iter.hasNext()) {
                    cp.advance();
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
     * 
     * When the bitmap is constructed from a ByteBuffer from a
     * memory-mapped file, this estimate is invalid: we can expect
     * the actual memory usage to be significantly (e.g., 10x) less.
     *
     * @return estimated memory usage.
     */
    public int getSizeInBytes() {
        int size = 2;
        
		MappeableContainerPointer cp = this.highLowContainer.getContainerPointer();
		while(cp.hasContainer()) {
			size += 4 +cp.getContainer().getSizeInBytes();
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
                    hs = BufferUtil.toIntUnsigned(ImmutableRoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
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
                throw new RuntimeException("Cannot modify.");
            }

        }.init();
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
            final int hs = BufferUtil.toIntUnsigned(this.highLowContainer.getKeyAtIndex(pos)) << 16;
            final MappeableContainer c = this.highLowContainer.getContainerAtIndex(pos++);
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

}