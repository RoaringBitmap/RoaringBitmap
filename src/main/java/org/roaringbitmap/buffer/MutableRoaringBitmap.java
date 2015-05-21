/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.ShortIterator;

import java.io.*;
import java.util.Iterator;

/**
 * MutableRoaringBitmap, a compressed alternative to the BitSet.
 * It is similar to org.roaringbitmap.RoaringBitmap, but it differs in that it
 * can interact with ImmutableRoaringBitmap objects.
 * 
 * <pre>
 * {@code
 *      import org.roaringbitmap.buffer.*;
 *       
 *      //...
 *      
 *      MutableRoaringBitmap rr = MutableRoaringBitmap.bitmapOf(1,2,3,1000);
 *      MutableRoaringBitmap rr2 = new MutableRoaringBitmap();
 *      for(int k = 4000; k<4255;++k) rr2.add(k);
 *      
 *      RoaringBitmap rror = RoaringBitmap.or(rr, rr2);
 * }
 * </pre>
 */
public class MutableRoaringBitmap extends ImmutableRoaringBitmap
        implements Cloneable, Serializable, Iterable<Integer>, Externalizable, BitmapDataProvider {
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
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

        while (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            if (s1 == s2) {
                final MappeableContainer c1 = x1.highLowContainer.getContainerAtIndex(pos1);
                final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                final MappeableContainer c = c1.and(c2);
                if (c.getCardinality() > 0) {
                    answer.getMappeableRoaringArray().append(s1, c);
                }
                ++pos1;
                ++pos2;
            } else if (s1 < s2) {
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
    public static MutableRoaringBitmap andNot(final MutableRoaringBitmap x1,
                                              final MutableRoaringBitmap x2) {
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
            } else if (s1 < s2) {
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
        final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
        final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
        final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
        final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));


        // copy the containers before the active area
        answer.getMappeableRoaringArray().appendCopiesUntil(
                bm.highLowContainer, (short) hbStart);

        for (int hb = hbStart; hb <= hbLast; ++hb) {
            final int containerStart = (hb == hbStart) ? lbStart : 0;
            final int containerLast = (hb == hbLast) ? lbLast : BufferUtil.maxLowBitAsInteger();

            final int i = bm.highLowContainer.getIndex((short) hb);
            final int j = answer.highLowContainer.getIndex((short) hb);
            assert j < 0;

            if (i >= 0) {
                final MappeableContainer c = bm.highLowContainer
                        .getContainerAtIndex(i).not(containerStart,
                                containerLast+1);
                if (c.getCardinality() > 0)
                    answer.getMappeableRoaringArray().insertNewKeyValueAt(
                            -j - 1, (short) hb, c);

            } else { // *think* the range of ones must never be
                // empty.
                answer.getMappeableRoaringArray().insertNewKeyValueAt(
                        -j - 1,
                        (short) hb,
                        MappeableContainer.rangeOfOnes(containerStart,
                                containerLast+1));
            }
        }
        // copy the containers after the active area.
        answer.getMappeableRoaringArray().appendCopiesAfter(
                bm.highLowContainer, (short) hbLast);

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
     * Add the value if it is not already present, otherwise remove it.
     * 
     * @param x integer value
     */
    public void flip(final int x) {
        final short hb = BufferUtil.highbits(x);
        final int i = highLowContainer.getIndex(hb);
        if (i >= 0) {
        	MappeableContainer c =  highLowContainer.getContainerAtIndex(i);
        	c = c.flip(BufferUtil.lowbits(x));
        	if(c.getCardinality()>0)
            ((MutableRoaringArray) highLowContainer).setContainerAtIndex(i,c);
        	else 
        		((MutableRoaringArray) highLowContainer).removeAtIndex(i);
        } else {
            final MappeableArrayContainer newac = new MappeableArrayContainer();
            ((MutableRoaringArray) highLowContainer).insertNewKeyValueAt(-i - 1, hb, newac.add(BufferUtil.lowbits(x)));
        }
    }
    
    /**
     * Add to the current bitmap all integers in [rangeStart,rangeEnd).
     *
     * @param rangeStart inclusive beginning of range
     * @param rangeEnd   exclusive ending of range
     */
    public void add(final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd)
            return; // empty range

        final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
        final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
        final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
        final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));        
        for (int hb = hbStart; hb <= hbLast; ++hb) {
            
            // first container may contain partial range
            final int containerStart = (hb == hbStart) ? lbStart : 0;
            // last container may contain partial range
            final int containerLast = (hb == hbLast) ? lbLast : BufferUtil.maxLowBitAsInteger();
            final int i = highLowContainer.getIndex((short) hb);

            if (i >= 0) {
                final MappeableContainer c = highLowContainer.getContainerAtIndex(i).iadd(
                               containerStart,  containerLast + 1);
                ((MutableRoaringArray) highLowContainer).setContainerAtIndex(i, c);
            } else {
                ((MutableRoaringArray) highLowContainer).insertNewKeyValueAt(-i - 1,(short) hb, MappeableContainer.rangeOfOnes(
                        containerStart, containerLast+1)
                );
            }
        }
    }

    /**
     * Generate a new bitmap with  all integers in [rangeStart,rangeEnd) added.
     * @param rb initial bitmap (will not be modified)
     * @param rangeStart inclusive beginning of range
     * @param rangeEnd   exclusive ending of range
     */
    public static MutableRoaringBitmap add(MutableRoaringBitmap rb, final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd)
            return rb.clone(); // empty range

        final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
        final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
        final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
        final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));

        MutableRoaringBitmap answer = new MutableRoaringBitmap();
        ((MutableRoaringArray) answer.highLowContainer).appendCopiesUntil(rb.highLowContainer, (short) hbStart);

        if(hbStart == hbLast) {
            final int i = rb.highLowContainer.getIndex((short) hbStart);
            final MappeableContainer c = i>=0 ? rb.highLowContainer.getContainerAtIndex(i).add(
                    lbStart, lbLast+1) : MappeableContainer.rangeOfOnes(lbStart, lbLast+1);
            ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
            ((MutableRoaringArray) answer.highLowContainer).appendCopiesAfter(rb.highLowContainer, (short) hbLast);
            return answer;
        }
        int ifirst = rb.highLowContainer.getIndex((short) hbStart);
        int ilast = rb.highLowContainer.getIndex((short) hbLast);

        {
            final MappeableContainer c = ifirst >=0? rb.highLowContainer.getContainerAtIndex(ifirst).add(
                     lbStart,BufferUtil.maxLowBitAsInteger()+1) : MappeableContainer.rangeOfOnes(lbStart, BufferUtil.maxLowBitAsInteger()+1) ;
            ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
        }
        for (int hb = hbStart + 1; hb < hbLast; ++hb) {
            MappeableContainer c = MappeableContainer.rangeOfOnes(0, BufferUtil.maxLowBitAsInteger()+1);
            ((MutableRoaringArray) answer.highLowContainer).append((short) hb, c);
        }
        {
            final MappeableContainer c = ilast >=0? rb.highLowContainer.getContainerAtIndex(ilast).add(
                     0,  lbLast+1) : MappeableContainer.rangeOfOnes(0,lbLast+1);
              ((MutableRoaringArray) answer.highLowContainer).append((short) hbLast,c);
        }
        ((MutableRoaringArray) answer.highLowContainer).appendCopiesAfter(rb.highLowContainer, (short) hbLast);
        return answer;
    }
    
    /**
     * Remove the current bitmap all integers in [rangeStart,rangeEnd).
     *
     * @param rangeStart inclusive beginning of range
     * @param rangeEnd   exclusive ending of range
     */
    public void remove(final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd)
            return; // empty range
        final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
        final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
        final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
        final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));        
        if(hbStart == hbLast) {
            final int i = highLowContainer.getIndex((short) hbStart);
            if(i < 0 ) return;
            final MappeableContainer c = highLowContainer.getContainerAtIndex(i).iremove(
                    lbStart, lbLast+1);
            if(c.getCardinality()>0)
                ((MutableRoaringArray) highLowContainer).setContainerAtIndex(i, c);
            else 
                ((MutableRoaringArray) highLowContainer).removeAtIndex(i);
            return;
        }
        int ifirst = highLowContainer.getIndex((short) hbStart);
        int ilast = highLowContainer.getIndex((short) hbLast);
        if(ifirst >=0) {
            if(lbStart != 0) {
               final MappeableContainer c = highLowContainer.getContainerAtIndex(ifirst).iremove(
                        lbStart,BufferUtil.maxLowBitAsInteger()+1) ;
               if(c.getCardinality()>0) {
                 ((MutableRoaringArray) highLowContainer).setContainerAtIndex(ifirst, c);
                 ifirst++;
                } 
            }
        } else {
            ifirst = - ifirst - 1;
        }
        if(ilast >=0) {
            if (lbLast != BufferUtil.maxLowBitAsInteger()) {
                final MappeableContainer c = highLowContainer.getContainerAtIndex(ilast).iremove(
                        0,  lbLast+1);
                if(c.getCardinality()>0) {
                    ((MutableRoaringArray) highLowContainer).setContainerAtIndex(ilast, c);
                } else ilast++;               
            } else ilast++;
        } else {
            ilast = - ilast -1;
        }
        ((MutableRoaringArray) highLowContainer).removeIndexRange(ifirst, ilast);
    }
    

    /**
     * Generate a new bitmap with  all integers in [rangeStart,rangeEnd) removed.
     * @param rb initial bitmap (will not be modified)
     * @param rangeStart inclusive beginning of range
     * @param rangeEnd   exclusive ending of range
     */
    public static MutableRoaringBitmap remove(MutableRoaringBitmap rb, final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd)
            return rb.clone(); // empty range
        final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
        final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
        final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
        final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));
        MutableRoaringBitmap answer = new MutableRoaringBitmap();
        ((MutableRoaringArray) answer.highLowContainer).appendCopiesUntil(rb.highLowContainer, (short) hbStart);

        if(hbStart == hbLast) {
            final int i = rb.highLowContainer.getIndex((short) hbStart);
            if (i >= 0) {
                final MappeableContainer c = rb.highLowContainer.getContainerAtIndex(i)
                        .remove(lbStart, lbLast + 1);
                if (c.getCardinality() > 0)
                    ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
            }
            ((MutableRoaringArray) answer.highLowContainer).appendCopiesAfter(rb.highLowContainer, (short) hbLast);
            return answer;
        }
        int ifirst = rb.highLowContainer.getIndex((short) hbStart);
        int ilast = rb.highLowContainer.getIndex((short) hbLast);
        if((ifirst >= 0) && (lbStart != 0)) {
            final MappeableContainer c = rb.highLowContainer.getContainerAtIndex(ifirst).remove(
                     lbStart,BufferUtil.maxLowBitAsInteger()+1);
           if(c.getCardinality()>0) {
              ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
           }
        }
        if((ilast >= 0) &&(lbLast != BufferUtil.maxLowBitAsInteger())) {
            final MappeableContainer c = rb.highLowContainer.getContainerAtIndex(ilast).remove(
                     0,  lbLast+1);
           if(c.getCardinality()>0) {
              ((MutableRoaringArray) answer.highLowContainer).append((short) hbLast,c);
           }
        }
        ((MutableRoaringArray) answer.highLowContainer).appendCopiesAfter(rb.highLowContainer, (short) hbLast);
        return answer;
    }


    /**
     * In-place bitwise AND (intersection) operation. The current bitmap is
     * modified.
     * 
     * @param array
     *            other bitmap
     */
    public void and(final ImmutableRoaringBitmap array) {
        int pos1 = 0, pos2 = 0, intersectionSize = 0;
        final int length1 = highLowContainer.size(), length2 = array.highLowContainer.size();

        while (pos1 < length1 && pos2 < length2) {
            final short s1 = highLowContainer.getKeyAtIndex(pos1);
            final short s2 = array.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                final MappeableContainer c1 = highLowContainer.getContainerAtIndex(pos1);
                final MappeableContainer c2 = array.highLowContainer.getContainerAtIndex(pos2);
                final MappeableContainer c = c1.iand(c2);
                if (c.getCardinality() > 0) {
                    getMappeableRoaringArray().replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
                }
                ++pos1;
                ++pos2;
            } else if (s1 < s2) {
                pos1 = highLowContainer.advanceUntil(s2,pos1);
            } else { // s1 > s2
                pos2 = array.highLowContainer.advanceUntil(s1,pos2);
            }
        }
        getMappeableRoaringArray().resize(intersectionSize);
    }

    /**
     * In-place bitwise ANDNOT (difference) operation. The current bitmap is
     * modified.
     * 
     * @param x2
     *            other bitmap
     */
    public void andNot(final ImmutableRoaringBitmap x2) {
        int pos1 = 0, pos2 = 0, intersectionSize = 0;
        final int length1 = highLowContainer.size(), length2 = x2.highLowContainer.size();

        while (pos1 < length1 && pos2 < length2) {
            final short s1 = highLowContainer.getKeyAtIndex(pos1);
            final short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                final MappeableContainer c1 = highLowContainer.getContainerAtIndex(pos1);
                final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                final MappeableContainer c = c1.iandNot(c2);
                if (c.getCardinality() > 0) {
                    getMappeableRoaringArray().replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
                }
                ++pos1;
                ++pos2;
            } else if (s1 < s2) {
                if(pos1 != intersectionSize) {
                    final MappeableContainer c1 = highLowContainer.getContainerAtIndex(pos1);
                    getMappeableRoaringArray().replaceKeyAndContainerAtIndex(intersectionSize, s1, c1);
                }
                ++intersectionSize;
                ++pos1;
            } else { // s1 > s2
                pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
            }
        }
        if (pos1 < length1) {
            getMappeableRoaringArray().copyRange(pos1, length1, intersectionSize);
            intersectionSize += length1 - pos1;
        }
        getMappeableRoaringArray().resize(intersectionSize);
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

        final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
        final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
        final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
        final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));
        
        for (int hb = hbStart; hb <= hbLast; ++hb) {
            // first container may contain partial range
            final int containerStart = (hb == hbStart) ? lbStart : 0;
            // last container may contain partial range
            final int containerLast = (hb == hbLast) ? lbLast : BufferUtil.maxLowBitAsInteger();
            final int i = highLowContainer.getIndex((short) hb);

            if (i >= 0) {
                final MappeableContainer c = highLowContainer
                        .getContainerAtIndex(i).inot(containerStart,
                                containerLast + 1);
                if (c.getCardinality() > 0)
                    getMappeableRoaringArray().setContainerAtIndex(i, c);
                else
                    getMappeableRoaringArray().removeAtIndex(i);
            } else {
                getMappeableRoaringArray().insertNewKeyValueAt(
                        -i - 1,
                        (short) hb,
                        MappeableContainer.rangeOfOnes(containerStart,
                                containerLast+1));
            }
        }
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
            private int hs = 0;

            private ShortIterator iter;

            private int pos = 0;

            private int x;

            @Override
            public boolean hasNext() {
                return pos < MutableRoaringBitmap.this.highLowContainer.size();
            }

            private Iterator<Integer> init() {
                if (pos < MutableRoaringBitmap.this.highLowContainer.size()) {
                    iter = MutableRoaringBitmap.this.highLowContainer
                            .getContainerAtIndex(pos).getShortIterator();
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
