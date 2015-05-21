/*
 * (c) the authors
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.io.*;
import java.util.Iterator;

/**
 * RoaringBitmap, a compressed alternative to the BitSet.
 *
 * <pre>
 * {@code
 *      import org.roaringbitmap.*;
 *
 *      //...
 *
 *      RoaringBitmap rr = RoaringBitmap.bitmapOf(1,2,3,1000);
 *      RoaringBitmap rr2 = new RoaringBitmap();
 *      for(int k = 4000; k<4255;++k) rr2.add(k);
 *      RoaringBitmap rror = RoaringBitmap.or(rr, rr2);
 * }
 * </pre>
 *
 *
 *
 */
public class RoaringBitmap implements Cloneable, Serializable, Iterable<Integer>, Externalizable, ImmutableBitmapDataProvider {

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
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
        int pos1 = 0, pos2 = 0;

        while (pos1 < length1 && pos2 < length2) {
            final short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            final short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                final Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
                final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                final Container c = c1.and(c2);
                if (c.getCardinality() > 0) {
                    answer.highLowContainer.append(s1, c);
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
	 * Sets the bits in the given range, from rangeStart (inclusive)
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
	public static RoaringBitmap set(RoaringBitmap bm, final int rangeStart,
			final int rangeEnd) {
		if (rangeStart >= rangeEnd)
			return bm.clone();

		RoaringBitmap answer = new RoaringBitmap();
		short hbStart = Util.highbits(rangeStart);
		final short lbStart = Util.lowbits(rangeStart);
		short hbLast = Util.highbits(rangeEnd - 1);
		final short lbLast = Util.lowbits(rangeEnd - 1);

		// copy the containers before the active area
		answer.highLowContainer.appendCopiesUntil(bm.highLowContainer, hbStart);

		final int max = Util.toIntUnsigned(Util.maxLowBit());
		final int containerStart = Util.toIntUnsigned(lbStart);
		final int containerLast = Util.toIntUnsigned(lbLast);
		// should be faster as we omit the previous conditional branchements
		// from inside the loop
		if (hbStart == hbLast) {
			setContainers(containerStart, containerLast, hbStart, hbLast,
					bm.highLowContainer, answer.highLowContainer);
		} else {
			setContainers(containerStart, max, hbStart, hbStart,
					bm.highLowContainer, answer.highLowContainer);
			setContainers(0, max, ++hbStart, --hbLast, bm.highLowContainer,
					answer.highLowContainer);
			hbLast++;
			setContainers(0, containerLast, hbLast, hbLast,
					bm.highLowContainer, answer.highLowContainer);
		}
		// copy the containers after the active area.
		answer.highLowContainer.appendCopiesAfter(bm.highLowContainer, hbLast);

		return answer;
	}

	/**
	 * In-place setting the bits in the given range, from rangeStart (inclusive)
	 * rangeEnd (exclusive). The given bitmap is will be changed.
	 * 
	 * @param bm
	 *            bitmap being negated
	 * @param rangeStart
	 *            inclusive beginning of range
	 * @param rangeEnd
	 *            exclusive ending of range
	 * @return a new Bitmap
	 */
	public void set(final int rangeStart, final int rangeEnd) {
		if (rangeStart >= rangeEnd)
			return; // empty range

		short hbStart = Util.highbits(rangeStart);
		final short lbStart = Util.lowbits(rangeStart);
		short hbLast = Util.highbits(rangeEnd - 1);
		final short lbLast = Util.lowbits(rangeEnd - 1);

		final int max = Util.toIntUnsigned(Util.maxLowBit());
		final int containerStart = Util.toIntUnsigned(lbStart);
		final int containerLast = Util.toIntUnsigned(lbLast);
		// should be faster as we omit the previous conditional branchements
		// from inside the loop
		if (hbStart == hbLast)
			isetContainers(containerStart, containerLast, hbStart, hbLast);
		else {
			isetContainers(containerStart, max, hbStart, hbStart);			
			isetContainers(0, max, ++hbStart, --hbLast);
			hbLast++;
			isetContainers(0, containerLast, hbLast, hbLast);
		}
	}

	private void isetContainers(int containerStart, int containerLast,
			short hbStart, short hbLast) {
		for (short hb = hbStart; hb <= hbLast; ++hb) {
			final int i = highLowContainer.getIndex(hb);

			if (i >= 0) {
				final Container c = highLowContainer.getContainerAtIndex(i)
						.iset(containerStart, containerLast);
				highLowContainer.setContainerAtIndex(i, c);
			} else {
				highLowContainer.insertNewKeyValueAt(-i - 1, hb,
						Container.rangeOfOnes(containerStart, containerLast));
			}
		}
	}

	private static void setContainers(int containerStart, int containerLast,
			short hbStart, short hbLast, RoaringArray bm, RoaringArray answer) {
		for (short hb = hbStart; hb <= hbLast; hb++) {
			final int i = bm.getIndex(hb);
			final int j = answer.getIndex(hb);
			assert j < 0;

			if (i >= 0) {
				final Container c = bm.getContainerAtIndex(i).set(
						containerStart, containerLast);		
				//it is supposed that the cardinality has grown
				answer.insertNewKeyValueAt(-j - 1, hb, c);
			} else {
				answer.insertNewKeyValueAt(-j - 1, hb,
						Container.rangeOfOnes(containerStart, containerLast));
			}
		}
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
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

        while (pos1 < length1 && pos2 < length2) {
            final short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            final short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                final Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
                final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                final Container c = c1.andNot(c2);
                if (c.getCardinality() > 0) {
                    answer.highLowContainer.append(s1, c);
                }
                ++pos1;
                ++pos2;
            } else if (s1 < s2) {
                final int nextPos1 = x1.highLowContainer.advanceUntil(s2,pos1);
                answer.highLowContainer.appendCopy(x1.highLowContainer, pos1, nextPos1);
                pos1 = nextPos1;
            } else { // s1 > s2
                pos2 = x2.highLowContainer.advanceUntil(s1,pos2);
            }
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
    public static RoaringBitmap flip(RoaringBitmap bm, final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd) {
            return bm.clone();
        }

        RoaringBitmap answer = new RoaringBitmap();
        final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
        final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
        final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
        final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));

        // copy the containers before the active area
        answer.highLowContainer.appendCopiesUntil(bm.highLowContainer, (short) hbStart);

        for (int hb = hbStart; hb <= hbLast; ++hb) {
            final int containerStart = (hb == hbStart) ? lbStart : 0;
            final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();

            final int i = bm.highLowContainer.getIndex((short) hb);
            final int j = answer.highLowContainer.getIndex((short) hb);
            assert j < 0;

            if (i >= 0) {
                Container c = bm.highLowContainer.getContainerAtIndex(i).not(containerStart, containerLast+1);                
                if (c.getCardinality() > 0)
                    answer.highLowContainer.insertNewKeyValueAt(-j - 1, (short) hb, c);

            } else { // *think* the range of ones must never be
                // empty.
                answer.highLowContainer.insertNewKeyValueAt(-j - 1, (short) hb, Container.rangeOfOnes(
                                containerStart, containerLast+1)
                );
            }
        }
        // copy the containers after the active area.
        answer.highLowContainer.appendCopiesAfter(bm.highLowContainer, (short) hbLast);
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
     * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be GetCardinality()).
     * @param x upper limit
     *
     * @return the rank
     */
    public int rank(int x) {
        int size = 0;
        int xhigh = Util.highbits(x);

        for (int i = 0; i < this.highLowContainer.size(); i++) {
            short key =  this.highLowContainer.getKeyAtIndex(i);
            if( key < xhigh )
              size += this.highLowContainer.getContainerAtIndex(i).getCardinality();
            else
                return size + this.highLowContainer.getContainerAtIndex(i).rank(Util.lowbits(x));
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
            Container c = this.highLowContainer.getContainerAtIndex(i);
            int thiscard = c.getCardinality();
            if(thiscard > leftover) {
                int keycontrib = this.highLowContainer.getKeyAtIndex(i)<<16;
                int lowcontrib = Util.toIntUnsigned(c.select(leftover));
                return  lowcontrib + keycontrib;
            }
            leftover -= thiscard;
        }
        throw new IllegalArgumentException("select "+j+" when the cardinality is "+this.getCardinality());
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
     * @return true if the added int wasn't already contained in the bitmap. False otherwise.
     */
    public boolean checkedAdd(final int x) {
        final short hb = Util.highbits(x);
        final int i = highLowContainer.getIndex(hb);
        if (i >= 0) {
        	Container c = highLowContainer.getContainerAtIndex(i);
        	int oldCard = c.getCardinality();        	
        	//we need to keep the newContainer if a switch between containers type
        	//occur, in order to get the new cardinality
        	c = c.add(Util.lowbits(x));
            highLowContainer.setContainerAtIndex(i, c);
            if(c.getCardinality()>oldCard)
            	return true;
        } else {
            final ArrayContainer newac = new ArrayContainer();
            highLowContainer.insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
            return true;
        }
        return false;
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
     * Add the value if it is not already present, otherwise remove it.
     * 
     * @param x integer value
     */
    public void flip(final int x) {
        final short hb = Util.highbits(x);
        final int i = highLowContainer.getIndex(hb);
        if (i >= 0) {
        	  Container c = highLowContainer.getContainerAtIndex(i).flip(Util.lowbits(x));
        	  if(c.getCardinality() > 0)
              highLowContainer.setContainerAtIndex(i,c);
        	  else
        	  	highLowContainer.removeAtIndex(i);
        } else {
            final ArrayContainer newac = new ArrayContainer();
            highLowContainer.insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
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

        final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
        final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
        final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
        final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));        
        for (int hb = hbStart; hb <= hbLast; ++hb) {
            
            // first container may contain partial range
            final int containerStart = (hb == hbStart) ? lbStart : 0;
            // last container may contain partial range
            final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();
            final int i = highLowContainer.getIndex((short) hb);

            if (i >= 0) {
                final Container c = highLowContainer.getContainerAtIndex(i).iadd(
                               containerStart,  containerLast + 1);
                highLowContainer.setContainerAtIndex(i, c);
            } else {
                highLowContainer.insertNewKeyValueAt(-i - 1,(short) hb, Container.rangeOfOnes(
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
    public static RoaringBitmap add(RoaringBitmap rb, final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd)
            return rb.clone(); // empty range

        final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
        final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
        final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
        final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));

        RoaringBitmap answer = new RoaringBitmap();
        answer.highLowContainer.appendCopiesUntil(rb.highLowContainer, (short) hbStart);

        if(hbStart == hbLast) {
            final int i = rb.highLowContainer.getIndex((short) hbStart);
            final Container c = i>=0 ? rb.highLowContainer.getContainerAtIndex(i).add(
                    lbStart, lbLast+1) : Container.rangeOfOnes(lbStart, lbLast+1);
            answer.highLowContainer.append((short) hbStart, c);
            answer.highLowContainer.appendCopiesAfter(rb.highLowContainer, (short) hbLast);
            return answer;
        }
        int ifirst = rb.highLowContainer.getIndex((short) hbStart);
        int ilast = rb.highLowContainer.getIndex((short) hbLast);

        {
            final Container c = ifirst >=0? rb.highLowContainer.getContainerAtIndex(ifirst).add(
                     lbStart,Util.maxLowBitAsInteger()+1) : Container.rangeOfOnes(lbStart, Util.maxLowBitAsInteger()+1) ;
            answer.highLowContainer.append((short) hbStart, c);
        }
        for (int hb = hbStart + 1; hb < hbLast; ++hb) {
            Container c = Container.rangeOfOnes(0, Util.maxLowBitAsInteger()+1);
            answer.highLowContainer.append((short) hb, c);
        }
        {
            final Container c = ilast >=0? rb.highLowContainer.getContainerAtIndex(ilast).add(
                     0,  lbLast+1) : Container.rangeOfOnes(0,lbLast+1);
              answer.highLowContainer.append((short) hbLast,c);
        }
        answer.highLowContainer.appendCopiesAfter(rb.highLowContainer, (short) hbLast);
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
        final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
        final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
        final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
        final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));        
        if(hbStart == hbLast) {
            final int i = highLowContainer.getIndex((short) hbStart);
            if(i < 0 ) return;
            final Container c = highLowContainer.getContainerAtIndex(i).iremove(
                    lbStart, lbLast+1);
            if(c.getCardinality()>0)
                highLowContainer.setContainerAtIndex(i, c);
            else 
                highLowContainer.removeAtIndex(i);
            return;
        }
        int ifirst = highLowContainer.getIndex((short) hbStart);
        int ilast = highLowContainer.getIndex((short) hbLast);
        if(ifirst >=0) {
            if(lbStart != 0) {
               final Container c = highLowContainer.getContainerAtIndex(ifirst).iremove(
                        lbStart,Util.maxLowBitAsInteger()+1) ;
               if(c.getCardinality()>0) {
                 highLowContainer.setContainerAtIndex(ifirst, c);
                 ifirst++;
                } 
            }
        } else {
            ifirst = - ifirst - 1;
        }
        if(ilast >=0) {
            if (lbLast != Util.maxLowBitAsInteger()) {
                final Container c = highLowContainer.getContainerAtIndex(ilast).iremove(
                        0,  lbLast+1);
                if(c.getCardinality()>0) {
                    highLowContainer.setContainerAtIndex(ilast, c);
                } else ilast++;               
            } else ilast++;
        } else {
            ilast = - ilast -1;
        }
        highLowContainer.removeIndexRange(ifirst, ilast);
    }
    

    /**
     * Generate a new bitmap with  all integers in [rangeStart,rangeEnd) removed.
     * @param rb initial bitmap (will not be modified)
     * @param rangeStart inclusive beginning of range
     * @param rangeEnd   exclusive ending of range
     */
    public static RoaringBitmap remove(RoaringBitmap rb, final int rangeStart, final int rangeEnd) {
        if (rangeStart >= rangeEnd)
            return rb.clone(); // empty range
        final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
        final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
        final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
        final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));
        RoaringBitmap answer = new RoaringBitmap();
        answer.highLowContainer.appendCopiesUntil(rb.highLowContainer, (short) hbStart);

        if(hbStart == hbLast) {
            final int i = rb.highLowContainer.getIndex((short) hbStart);
            if (i >= 0) {
                final Container c = rb.highLowContainer.getContainerAtIndex(i)
                        .remove(lbStart, lbLast + 1);
                if (c.getCardinality() > 0)
                    answer.highLowContainer.append((short) hbStart, c);
            }
            answer.highLowContainer.appendCopiesAfter(rb.highLowContainer, (short) hbLast);
            return answer;
        }
        int ifirst = rb.highLowContainer.getIndex((short) hbStart);
        int ilast = rb.highLowContainer.getIndex((short) hbLast);
        if((ifirst >= 0) && (lbStart != 0)) {
            final Container c = rb.highLowContainer.getContainerAtIndex(ifirst).remove(
                     lbStart,Util.maxLowBitAsInteger()+1);
           if(c.getCardinality()>0) {
              answer.highLowContainer.append((short) hbStart, c);
           }
        }
        if((ilast >= 0) &&(lbLast != Util.maxLowBitAsInteger())) {
            final Container c = rb.highLowContainer.getContainerAtIndex(ilast).remove(
                     0,  lbLast+1);
           if(c.getCardinality()>0) {
              answer.highLowContainer.append((short) hbLast,c);
           }
        }
        answer.highLowContainer.appendCopiesAfter(rb.highLowContainer, (short) hbLast);
        return answer;
    }

    /**
     * In-place bitwise AND (intersection) operation. The current bitmap is
     * modified.
     *
     * @param x2 other bitmap
     */
    public void and(final RoaringBitmap x2) {
        int pos1 = 0, pos2 = 0, intersectionSize = 0;
        final int length1 = highLowContainer.size(), length2 = x2.highLowContainer.size();

        while (pos1 < length1 && pos2 < length2) {
            final short s1 = highLowContainer.getKeyAtIndex(pos1);
            final short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                final Container c1 = highLowContainer.getContainerAtIndex(pos1);
                final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                final Container c = c1.iand(c2);
                if (c.getCardinality() > 0) {
                    highLowContainer.replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
                }
                ++pos1;
                ++pos2;
            } else if (s1 < s2) {
                pos1 = highLowContainer.advanceUntil(s2,pos1);
            } else { // s1 > s2
                pos2 = x2.highLowContainer.advanceUntil(s1,pos2);
            }
        }
        highLowContainer.resize(intersectionSize);
    }

    /**
     * In-place bitwise ANDNOT (difference) operation. The current bitmap is
     * modified.
     *
     * @param x2 other bitmap
     */
    public void andNot(final RoaringBitmap x2) {
        int pos1 = 0, pos2 = 0, intersectionSize = 0;
        final int length1 = highLowContainer.size(), length2 = x2.highLowContainer.size();

        while (pos1 < length1 && pos2 < length2) {
            final short s1 = highLowContainer.getKeyAtIndex(pos1);
            final short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                final Container c1 = highLowContainer.getContainerAtIndex(pos1);
                final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                final Container c = c1.iandNot(c2);
                if (c.getCardinality() > 0) {
                    highLowContainer.replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
                }
                ++pos1;
                ++pos2;
            } else if (s1 < s2) {
                if(pos1 != intersectionSize) {
                    final Container c1 = highLowContainer.getContainerAtIndex(pos1);
                    highLowContainer.replaceKeyAndContainerAtIndex(intersectionSize, s1, c1);
                }
                ++intersectionSize;
                ++pos1;
            } else { // s1 > s2
                pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
            }
        }
        if (pos1 < length1) {
            highLowContainer.copyRange(pos1, length1, intersectionSize);
            intersectionSize += length1 - pos1;
        }
        highLowContainer.resize(intersectionSize);
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

        final int hbStart = Util.toIntUnsigned(Util.highbits(rangeStart));
        final int lbStart = Util.toIntUnsigned(Util.lowbits(rangeStart));
        final int hbLast = Util.toIntUnsigned(Util.highbits(rangeEnd - 1));
        final int lbLast = Util.toIntUnsigned(Util.lowbits(rangeEnd - 1));

        // TODO:this can be accelerated considerably
        for (int hb = hbStart; hb <= hbLast; ++hb) {
            // first container may contain partial range
            final int containerStart = (hb == hbStart) ? lbStart : 0;
            // last container may contain partial range
            final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();
            final int i = highLowContainer.getIndex((short) hb);

            if (i >= 0) {
            	final Container c = highLowContainer.getContainerAtIndex(i).inot(
                                containerStart, containerLast+1);
                if (c.getCardinality() > 0)
                    highLowContainer.setContainerAtIndex(i, c);
                else
                    highLowContainer.removeAtIndex(i);
            } else {
                highLowContainer.insertNewKeyValueAt(-i - 1,(short) hb, Container.rangeOfOnes(
                        containerStart, containerLast+1)
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
     * @return a custom iterator over set bits, the bits are traversed
     * in ascending sorted order
     */
    public IntIterator getIntIterator() {
        return new RoaringIntIterator();
    }

    /**
     * @return a custom iterator over set bits, the bits are traversed
     * in descending sorted order
     */
    public IntIterator getReverseIntIterator() {
        return new RoaringReverseIntIterator();
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
            private int hs = 0;

            private ShortIterator iter;

            private int pos = 0;

            private int x;

            @Override
            public boolean hasNext() {
                return pos < RoaringBitmap.this.highLowContainer.size();
            }

            private Iterator<Integer> init() {
                if (pos < RoaringBitmap.this.highLowContainer.size()) {
                    iter = RoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).getShortIterator();
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
     * If present remove the specified integers (effectively, sets its bit
     * value to false)
     *
     * @param x integer value representing the index in a bitmap
     * @return true if the unset bit was already in the bitmap
     */
    public boolean checkedRemove(final int x) {
        final short hb = Util.highbits(x);
        final int i = highLowContainer.getIndex(hb);
        if (i < 0)
            return false;
        Container c = highLowContainer.getContainerAtIndex(i);
        int oldCard = c.getCardinality();
        c = c.remove(Util.lowbits(x));
        int newCard = c.getCardinality();
        if(newCard==0){
        	highLowContainer.removeAtIndex(i);
        	return true;//can't be 0 before a remove
        }
        if(oldCard>newCard){
        	highLowContainer.setContainerAtIndex(i, c);
        	return true;
        }
        return false;
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
     * Create a new Roaring bitmap containing at most maxcardinality integers.
     *
     * @param maxcardinality maximal cardinality
     * @return a new bitmap with cardinality no more than maxcardinality
     */
    public RoaringBitmap limit(int maxcardinality) {
        RoaringBitmap answer = new RoaringBitmap();
        int currentcardinality = 0;
        for (int i = 0; (currentcardinality < maxcardinality) && ( i < this.highLowContainer.size()); i++) {
            Container c = this.highLowContainer.getContainerAtIndex(i);
            if(c.getCardinality() + currentcardinality <= maxcardinality) {
               answer.highLowContainer.appendCopy(this.highLowContainer, i);
               currentcardinality += c.getCardinality();
            }  else {
                int leftover = maxcardinality - currentcardinality;
                Container limited = c.limit(leftover);
                answer.highLowContainer.append(this.highLowContainer.getKeyAtIndex(i),limited );
                break;
            }
        }
        return answer;
    }

    /**
     * Return the set values as an array. The integer
     * values are in sorted order.
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


    private final class RoaringIntIterator implements IntIterator {
        private int hs = 0;

        private ShortIterator iter;

        private int pos = 0;

        private int x;

        private RoaringIntIterator() {
            nextContainer();
        }

        @Override
        public boolean hasNext() {
            return pos < RoaringBitmap.this.highLowContainer.size();
        }

        private void nextContainer() {
            if (pos < RoaringBitmap.this.highLowContainer.size()) {
                iter = RoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).getShortIterator();
                hs = Util.toIntUnsigned(RoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
            }
        }

        @Override
        public int next() {
            x = Util.toIntUnsigned(iter.next()) | hs;
            if (!iter.hasNext()) {
                ++pos;
                nextContainer();
            }
            return x;
        }

        @Override
        public IntIterator clone() {
            try {
                RoaringIntIterator x = (RoaringIntIterator) super.clone();
                x.iter =  this.iter.clone();
                return x;
            } catch (CloneNotSupportedException e) {
                return null;// will not happen
            }
        }

    }

    private final class RoaringReverseIntIterator implements IntIterator {

        int hs = 0;
        ShortIterator iter;
        // don't need an int because we go to 0, not Short.MAX_VALUE, and signed shorts underflow well below zero
        short pos = (short) (RoaringBitmap.this.highLowContainer.size() - 1);

        private RoaringReverseIntIterator() {
            nextContainer();
        }

        @Override
        public boolean hasNext() {
            return pos >= 0;
        }

        private void nextContainer() {
            if (pos >= 0) {
                iter = RoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).getReverseShortIterator();
                hs = Util.toIntUnsigned(RoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
            }
        }

        @Override
        public int next() {
            final int x = Util.toIntUnsigned(iter.next()) | hs;
            if (!iter.hasNext()) {
                --pos;
                nextContainer();
            }
            return x;
        }

        @Override
        public IntIterator clone() {
            try {
                RoaringReverseIntIterator clone = (RoaringReverseIntIterator) super.clone();
                clone.iter =  this.iter.clone();
                return clone;
            } catch (CloneNotSupportedException e) {
                return null;// will not happen
            }
        }

    }
}
