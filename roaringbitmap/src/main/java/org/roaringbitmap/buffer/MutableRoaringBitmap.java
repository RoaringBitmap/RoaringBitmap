/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.ContainerPointer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ShortIterator;
import org.roaringbitmap.InvalidRoaringFormat;
import org.roaringbitmap.Util;

import java.io.*;
import java.util.Iterator;

/**
 * MutableRoaringBitmap, a compressed alternative to the BitSet. It is similar to
 * org.roaringbitmap.RoaringBitmap, but it differs in that it can interact with
 * ImmutableRoaringBitmap objects in the sense that MutableRoaringBitmap is
 * derived from MutableRoaringBitmap.
 *
 * A MutableRoaringBitmap is an instance of an ImmutableRoaringBitmap (where methods like
 * "serialize" are implemented). That is, they both share the same core (immutable) methods, but a
 * MutableRoaringBitmap adds methods that allow you to modify the object. This design allows us to
 * use MutableRoaringBitmap as ImmutableRoaringBitmap instances when needed.
 * MutableRoaringBitmap instances can be casted to an ImmutableRoaringBitmap instance
 * in constant time which means that code written for ImmutableRoaringBitmap instances
 * run at full speed (without copies) on MutableRoaringBitmap
 * instances.
 *
 * A MutableRoaringBitmap can be used much like an org.roaringbitmap.RoaringBitmap instance, and
 * they serialize to the same output. The RoaringBitmap instance will be faster since it does not
 * carry the overhead of a ByteBuffer back-end, but the MutableRoaringBitmap can be used as an
 * ImmutableRoaringBitmap instance. Thus, if you use ImmutableRoaringBitmap, you probably need to
 * use MutableRoaringBitmap instances as well; if you do not use ImmutableRoaringBitmap, you
 * probably want to use only RoaringBitmap instances.
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
 *
 *      //...
 *      DataOutputStream wheretoserialize = ...
 *      rr.runOptimize(); // can help compression
 *      rr.serialize(wheretoserialize);
 * }
 * </pre>
 *
 *
 * Integers are added in unsigned sorted order. That is, they
 * are treated as unsigned integers (see
 * Java 8's Integer.toUnsignedLong function). Up to 4294967296 integers
 * can be stored.
 *
 * @see ImmutableRoaringBitmap
 * @see org.roaringbitmap.RoaringBitmap
 */
public class MutableRoaringBitmap extends ImmutableRoaringBitmap
    implements Cloneable, Serializable, Iterable<Integer>, Externalizable, BitmapDataProvider {
  private static final long serialVersionUID = 4L; // 3L; bumped by ofk for runcontainers

  /**
   * Generate a new bitmap that has the same cardinality as x, but with
   * all its values incremented by offset.
   * 
   * @param x source bitmap
   * @param offset increment
   * @return a new bitmap
   */
  public static MutableRoaringBitmap addOffset(final ImmutableRoaringBitmap x, int offset) {
    int container_offset = offset >>> 16;
    int in_container_offset = offset % (1<<16);
    if(in_container_offset == 0) {
      MutableRoaringBitmap answer = x.toMutableRoaringBitmap();
      for(int pos = 0; pos < answer.highLowContainer.size(); pos++) {
        int key = BufferUtil.toIntUnsigned(answer.getMappeableRoaringArray()
            .getKeyAtIndex(pos));
        key += container_offset;
        if(key > 0xFFFF) {
          throw new IllegalArgumentException("Offset too large.");
        }
        answer.getMappeableRoaringArray().keys[pos] = (short) key;
      }
      return answer;
    } else {
      MutableRoaringBitmap answer = new MutableRoaringBitmap();
      for(int pos = 0; pos < x.highLowContainer.size(); pos++) {
        int key = BufferUtil.toIntUnsigned(x.highLowContainer.getKeyAtIndex(pos));
        key += container_offset;
        if(key > 0xFFFF) {
          throw new IllegalArgumentException("Offset too large.");
        }
        MappeableContainer c = x.highLowContainer.getContainerAtIndex(pos);
        MappeableContainer[] offsetted = BufferUtil.addOffset(c, 
          (short)in_container_offset);
        if( !offsetted[0].isEmpty()) {
          int current_size = answer.highLowContainer.size();
          int lastkey = 0;
          if(current_size > 0) {
            lastkey = answer.highLowContainer.getKeyAtIndex(
              current_size - 1);
          }
          if((current_size > 0) && (lastkey == key)) {
            MappeableContainer prev = answer.highLowContainer
                .getContainerAtIndex(current_size - 1);
            MappeableContainer orresult = prev.ior(offsetted[0]);
            answer.getMappeableRoaringArray().setContainerAtIndex(current_size - 1,
                orresult);
          } else {
            answer.getMappeableRoaringArray().append((short)key, offsetted[0]);
          }
        }
        if( !offsetted[1].isEmpty()) {
          if(key == 0xFFFF) {
            throw new IllegalArgumentException("Offset too large.");
          }
          answer.getMappeableRoaringArray().append((short)(key + 1), offsetted[1]);
        }
      }
      answer.repairAfterLazy();
      return answer;
    }
  }

  /**
   * Generate a new bitmap with all integers in [rangeStart,rangeEnd) added.
   *
   * @param rb initial bitmap (will not be modified)
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new bitmap
   */
  public static MutableRoaringBitmap add(MutableRoaringBitmap rb, final long rangeStart,
      final long rangeEnd) {
    rangeSanityCheck(rangeStart,rangeEnd);
    if (rangeStart >= rangeEnd) {
      return rb.clone(); // empty range
    }

    final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
    final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
    final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
    final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));

    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    ((MutableRoaringArray) answer.highLowContainer).appendCopiesUntil(rb.highLowContainer,
        (short) hbStart);

    if (hbStart == hbLast) {
      final int i = rb.highLowContainer.getIndex((short) hbStart);
      final MappeableContainer c =
          i >= 0 ? rb.highLowContainer.getContainerAtIndex(i).add(lbStart, lbLast + 1)
              : MappeableContainer.rangeOfOnes(lbStart, lbLast + 1);
      ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
      ((MutableRoaringArray) answer.highLowContainer).appendCopiesAfter(rb.highLowContainer,
          (short) hbLast);
      return answer;
    }
    int ifirst = rb.highLowContainer.getIndex((short) hbStart);
    int ilast = rb.highLowContainer.getIndex((short) hbLast);

    {
      final MappeableContainer c = ifirst >= 0
          ? rb.highLowContainer.getContainerAtIndex(ifirst).add(lbStart,
              BufferUtil.maxLowBitAsInteger() + 1)
          : MappeableContainer.rangeOfOnes(lbStart, BufferUtil.maxLowBitAsInteger() + 1);
      ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
    }
    for (int hb = hbStart + 1; hb < hbLast; ++hb) {
      MappeableContainer c = MappeableContainer.rangeOfOnes(0, BufferUtil.maxLowBitAsInteger() + 1);
      ((MutableRoaringArray) answer.highLowContainer).append((short) hb, c);
    }
    {
      final MappeableContainer c =
          ilast >= 0 ? rb.highLowContainer.getContainerAtIndex(ilast).add(0, lbLast + 1)
              : MappeableContainer.rangeOfOnes(0, lbLast + 1);
      ((MutableRoaringArray) answer.highLowContainer).append((short) hbLast, c);
    }
    ((MutableRoaringArray) answer.highLowContainer).appendCopiesAfter(rb.highLowContainer,
        (short) hbLast);
    return answer;
  }

  /**
   *
   * Generate a new bitmap with all integers in [rangeStart,rangeEnd) added.
   *
   * @param rb initial bitmap (will not be modified)
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new bitmap
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
  public static MutableRoaringBitmap add(MutableRoaringBitmap rb,
                                           final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      return add(rb, (long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    return add(rb, rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL);
  }




  /**
   * Bitwise AND (intersection) operation. The provided bitmaps are *not* modified. This operation
   * is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   */
  public static MutableRoaringBitmap and(final MutableRoaringBitmap x1,
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
        final MappeableContainer c = c1.and(c2);
        if (!c.isEmpty()) {
          answer.getMappeableRoaringArray().append(s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
        pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
      } else { // s1 > s2
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    return answer;
  }

  /**
   * Bitwise ANDNOT (difference) operation. The provided bitmaps are *not* modified. This operation
   * is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
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
        if (!c.isEmpty()) {
          answer.getMappeableRoaringArray().append(s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
        final int nextPos1 = x1.highLowContainer.advanceUntil(s2, pos1);
        answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer, pos1, nextPos1);
        pos1 = nextPos1;
      } else { // s1 > s2
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    if (pos2 == length2) {
      answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer, pos1, length1);
    }
    return answer;
  }
  /**
   * Set all the specified values  to true. This can be expected to be slightly
   * faster than calling "add" repeatedly. The provided integers values don't
   * have to be in sorted order, but it may be preferable to sort them from a performance point of
   * view.
   *
   * @param dat set values
   */
  public void add(final int... dat) {
    MutableRoaringArray mra = (MutableRoaringArray) highLowContainer;
    MappeableContainer currentcont = null;
    short currenthb = 0;
    int currentcontainerindex = 0;
    int j = 0;
    if(j < dat.length) {
      int val = dat[j];
      currenthb = BufferUtil.highbits(val);
      currentcontainerindex = highLowContainer.getIndex(currenthb);
      if (currentcontainerindex >= 0) {
        currentcont = highLowContainer.getContainerAtIndex(currentcontainerindex);
        MappeableContainer newcont = currentcont.add(BufferUtil.lowbits(val));
        if(newcont != currentcont) {
          mra.setContainerAtIndex(currentcontainerindex, newcont);
          currentcont = newcont;
        }
      } else {
        currentcontainerindex = - currentcontainerindex - 1;
        final MappeableArrayContainer newac = new MappeableArrayContainer();
        currentcont = newac.add(BufferUtil.lowbits(val));
        mra.insertNewKeyValueAt(currentcontainerindex, currenthb, currentcont);
      }
      j++;
    }
    for( ; j < dat.length; ++j) {
      int val = dat[j];
      short newhb = BufferUtil.highbits(val);
      if(currenthb == newhb) {// easy case
        // this could be quite frequent
        MappeableContainer newcont = currentcont.add(BufferUtil.lowbits(val));
        if(newcont != currentcont) {
          mra.setContainerAtIndex(currentcontainerindex, newcont);
          currentcont = newcont;
        }
      } else {
        currenthb = newhb;
        currentcontainerindex = highLowContainer.getIndex(currenthb);
        if (currentcontainerindex >= 0) {
          currentcont = highLowContainer.getContainerAtIndex(currentcontainerindex);
          MappeableContainer newcont = currentcont.add(BufferUtil.lowbits(val));
          if(newcont != currentcont) {
            mra.setContainerAtIndex(currentcontainerindex, newcont);
            currentcont = newcont;
          }
        } else {
          currentcontainerindex = - currentcontainerindex - 1;
          final MappeableArrayContainer newac = new MappeableArrayContainer();
          currentcont = newac.add(BufferUtil.lowbits(val));
          mra.insertNewKeyValueAt(currentcontainerindex, currenthb, currentcont);
        }
      }
    }
  }

  /**
   * Generate a bitmap with the specified values set to true. The provided integers values don't
   * have to be in sorted order, but it may be preferable to sort them from a performance point of
   * view.
   *
   * @param dat set values
   * @return a new bitmap
   */
  public static MutableRoaringBitmap bitmapOf(final int... dat) {
    final MutableRoaringBitmap ans = new MutableRoaringBitmap();
    ans.add(dat);
    return ans;
  }



  protected static void rangeSanityCheck(final long rangeStart, final long rangeEnd) {
    if (rangeStart < 0 || rangeStart > (1L << 32)-1) {
      throw new IllegalArgumentException("rangeStart="+ rangeStart
                                         +" should be in [0, 0xffffffff]");
    }
    if (rangeEnd > (1L << 32) || rangeEnd < 0) {
      throw new IllegalArgumentException("rangeEnd="+ rangeEnd
                                         +" should be in [0, 0xffffffff + 1]");
    }
  }

  /**
   * Complements the bits in the given range, from rangeStart (inclusive) rangeEnd (exclusive). The
   * given bitmap is unchanged.
   *
   * @param bm bitmap being negated
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return a new Bitmap
   */
  public static MutableRoaringBitmap flip(MutableRoaringBitmap bm, final long rangeStart,
      final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return bm.clone();
    }

    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
    final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
    final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
    final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));


    // copy the containers before the active area
    answer.getMappeableRoaringArray().appendCopiesUntil(bm.highLowContainer, (short) hbStart);

    for (int hb = hbStart; hb <= hbLast; ++hb) {
      final int containerStart = (hb == hbStart) ? lbStart : 0;
      final int containerLast = (hb == hbLast) ? lbLast : BufferUtil.maxLowBitAsInteger();

      final int i = bm.highLowContainer.getIndex((short) hb);
      final int j = answer.highLowContainer.getIndex((short) hb);
      assert j < 0;

      if (i >= 0) {
        final MappeableContainer c =
            bm.highLowContainer.getContainerAtIndex(i).not(containerStart, containerLast + 1);
        if (!c.isEmpty()) {
          answer.getMappeableRoaringArray().insertNewKeyValueAt(-j - 1, (short) hb, c);
        }

      } else { // *think* the range of ones must never be
        // empty.
        answer.getMappeableRoaringArray().insertNewKeyValueAt(-j - 1, (short) hb,
            MappeableContainer.rangeOfOnes(containerStart, containerLast + 1));
      }
    }
    // copy the containers after the active area.
    answer.getMappeableRoaringArray().appendCopiesAfter(bm.highLowContainer, (short) hbLast);

    return answer;
  }


  /**
   * Complements the bits in the given range, from rangeStart (inclusive) rangeEnd (exclusive). The
   * given bitmap is unchanged.
   *
   * @param rb bitmap being negated
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return a new Bitmap
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
  public static MutableRoaringBitmap flip(MutableRoaringBitmap rb,
                                            final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      return flip(rb, (long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    return flip(rb, rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL);
  }

  // important: inputs should not have been computed lazily
  protected static MutableRoaringBitmap lazyorfromlazyinputs(final MutableRoaringBitmap x1,
      final MutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
    MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();
    main: if (i1.hasContainer() && i2.hasContainer()) {
      while (true) {
        if (i1.key() == i2.key()) {
          MappeableContainer c1 = i1.getContainer();
          MappeableContainer c2 = i2.getContainer();
          if ((c2 instanceof MappeableBitmapContainer)
              && (!(c1 instanceof MappeableBitmapContainer))) {
            MappeableContainer tmp = c1;
            c1 = c2;
            c2 = tmp;
          }
          answer.getMappeableRoaringArray().append(i1.key(), c1.lazyIOR(c2));
          i1.advance();
          i2.advance();
          if (!i1.hasContainer() || !i2.hasContainer()) {
            break main;
          }
        } else if (Util.compareUnsigned(i1.key(), i2.key()) < 0) { // i1.key() < i2.key()
          answer.getMappeableRoaringArray().append(i1.key(), i1.getContainer());
          i1.advance();
          if (!i1.hasContainer()) {
            break main;
          }
        } else { // i1.key() > i2.key()
          answer.getMappeableRoaringArray().append(i2.key(), i2.getContainer());
          i2.advance();
          if (!i2.hasContainer()) {
            break main;
          }
        }
      }
    }
    if (!i1.hasContainer()) {
      while (i2.hasContainer()) {
        answer.getMappeableRoaringArray().append(i2.key(), i2.getContainer());
        i2.advance();
      }
    } else if (!i2.hasContainer()) {
      while (i1.hasContainer()) {
        answer.getMappeableRoaringArray().append(i1.key(), i1.getContainer());
        i1.advance();
      }
    }
    return answer;
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
   * Bitwise OR (union) operation. The provided bitmaps are *not* modified. This operation is
   * thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   */
  public static MutableRoaringBitmap or(final MutableRoaringBitmap x1,
      final MutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          answer.getMappeableRoaringArray().append(s1, x1.highLowContainer.getContainerAtIndex(pos1)
              .or(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
          answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer.getKeyAtIndex(pos1),
              x1.highLowContainer.getContainerAtIndex(pos1));
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
        } else { // s1 > s2
          answer.getMappeableRoaringArray().appendCopy(x2.highLowContainer.getKeyAtIndex(pos2),
              x2.highLowContainer.getContainerAtIndex(pos2));
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      answer.getMappeableRoaringArray().appendCopy(x2.highLowContainer, pos2, length2);
    } else if (pos2 == length2) {
      answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer, pos1, length1);
    }
    return answer;
  }

  /**
   * Generate a new bitmap with all integers in [rangeStart,rangeEnd) removed.
   *
   * @param rb initial bitmap (will not be modified)
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new bitmap
   */
  public static MutableRoaringBitmap remove(MutableRoaringBitmap rb, final long rangeStart,
      final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return rb.clone(); // empty range
    }
    final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
    final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
    final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
    final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));
    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    ((MutableRoaringArray) answer.highLowContainer).appendCopiesUntil(rb.highLowContainer,
        (short) hbStart);

    if (hbStart == hbLast) {
      final int i = rb.highLowContainer.getIndex((short) hbStart);
      if (i >= 0) {
        final MappeableContainer c =
            rb.highLowContainer.getContainerAtIndex(i).remove(lbStart, lbLast + 1);
        if (!c.isEmpty()) {
          ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
        }
      }
      ((MutableRoaringArray) answer.highLowContainer).appendCopiesAfter(rb.highLowContainer,
          (short) hbLast);
      return answer;
    }
    int ifirst = rb.highLowContainer.getIndex((short) hbStart);
    int ilast = rb.highLowContainer.getIndex((short) hbLast);
    if ((ifirst >= 0) && (lbStart != 0)) {
      final MappeableContainer c = rb.highLowContainer.getContainerAtIndex(ifirst).remove(lbStart,
          BufferUtil.maxLowBitAsInteger() + 1);
      if (!c.isEmpty()) {
        ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
      }
    }
    if ((ilast >= 0) && (lbLast != BufferUtil.maxLowBitAsInteger())) {
      final MappeableContainer c =
          rb.highLowContainer.getContainerAtIndex(ilast).remove(0, lbLast + 1);
      if (!c.isEmpty()) {
        ((MutableRoaringArray) answer.highLowContainer).append((short) hbLast, c);
      }
    }
    ((MutableRoaringArray) answer.highLowContainer).appendCopiesAfter(rb.highLowContainer,
        (short) hbLast);
    return answer;
  }

  /**
   * Generate a new bitmap with all integers in [rangeStart,rangeEnd) removed.
   *
   * @param rb initial bitmap (will not be modified)
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new bitmap
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
  public static MutableRoaringBitmap remove(MutableRoaringBitmap rb,
                                              final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      return remove(rb, (long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    return remove(rb, rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL);
  }


  /**
   * Bitwise XOR (symmetric difference) operation. The provided bitmaps are *not* modified. This
   * operation is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   */
  public static MutableRoaringBitmap xor(final MutableRoaringBitmap x1,
      final MutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    main: if (pos1 < length1 && pos2 < length2) {
      short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          final MappeableContainer c = x1.highLowContainer.getContainerAtIndex(pos1)
              .xor(x2.highLowContainer.getContainerAtIndex(pos2));
          if (!c.isEmpty()) {
            answer.getMappeableRoaringArray().append(s1, c);
          }
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
          answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer.getKeyAtIndex(pos1),
              x1.highLowContainer.getContainerAtIndex(pos1));
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
        } else if (Util.compareUnsigned(s1, s2) > 0) {
          answer.getMappeableRoaringArray().appendCopy(x2.highLowContainer.getKeyAtIndex(pos2),
              x2.highLowContainer.getContainerAtIndex(pos2));
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      answer.getMappeableRoaringArray().appendCopy(x2.highLowContainer, pos2, length2);
    } else if (pos2 == length2) {
      answer.getMappeableRoaringArray().appendCopy(x1.highLowContainer, pos1, length1);
    }
    return answer;
  }

  /**
   * Create an empty bitmap
   */
  public MutableRoaringBitmap() {
    this(new MutableRoaringArray());
  }

  public MutableRoaringBitmap(MutableRoaringArray highLowContainer) {
    this.highLowContainer = highLowContainer;
  }

  /**
   * Create a MutableRoaringBitmap from a RoaringBitmap. The RoaringBitmap is not modified.
   *
   * @param rb the original bitmap
   */
  public MutableRoaringBitmap(RoaringBitmap rb) {
    highLowContainer = new MutableRoaringArray();
    ContainerPointer cp = rb.getContainerPointer();
    while (cp.getContainer() != null) {
      ((MutableRoaringArray) highLowContainer).append(cp.key(),
          cp.getContainer().toMappeableContainer());
      cp.advance();
    }
  }

  /**
   * Add the value to the container (set the value to "true"), whether it already appears or not.
   *
   * Java lacks native unsigned integers but the x argument is considered to be unsigned.
   * Within bitmaps, numbers are ordered according to {@link Integer#compareUnsigned}.
   * We order the numbers like 0, 1, ..., 2147483647, -2147483648, -2147483647,..., -1.
   *
   * @param x integer value
   */
  @Override
  public void add(final int x) {
    final short hb = BufferUtil.highbits(x);
    final int i = highLowContainer.getIndex(hb);
    if (i >= 0) {
      getMappeableRoaringArray().setContainerAtIndex(i,
          highLowContainer.getContainerAtIndex(i).add(BufferUtil.lowbits(x)));
    } else {
      final MappeableArrayContainer newac = new MappeableArrayContainer();
      getMappeableRoaringArray().insertNewKeyValueAt(-i - 1, hb, newac.add(BufferUtil.lowbits(x)));
    }
  }

  /**
   * Add to the current bitmap all integers in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   */
  public void add(final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return; // empty range
    }

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
        final MappeableContainer c =
            highLowContainer.getContainerAtIndex(i).iadd(containerStart, containerLast + 1);
        ((MutableRoaringArray) highLowContainer).setContainerAtIndex(i, c);
      } else {
        ((MutableRoaringArray) highLowContainer).insertNewKeyValueAt(-i - 1, (short) hb,
            MappeableContainer.rangeOfOnes(containerStart, containerLast + 1));
      }
    }
  }

 /**
   *
   * Add to the current bitmap all integers in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
  public void add(final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      add((long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    add(rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL);
  }




  /**
   * In-place bitwise AND (intersection) operation. The current bitmap is modified.
   *
   * @param array other bitmap
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
        if (!c.isEmpty()) {
          getMappeableRoaringArray().replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
        pos1 = highLowContainer.advanceUntil(s2, pos1);
      } else { // s1 > s2
        pos2 = array.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    getMappeableRoaringArray().resize(intersectionSize);
  }


  /**
   * In-place bitwise ANDNOT (difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
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
        if (!c.isEmpty()) {
          getMappeableRoaringArray().replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
        if (pos1 != intersectionSize) {
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
   * Add the value to the container (set the value to "true"), whether it already appears or not.
   *
   * @param x integer value
   * @return true if the added int wasn't already contained in the bitmap. False otherwise.
   */
  public boolean checkedAdd(final int x) {
    final short hb = BufferUtil.highbits(x);
    final int i = highLowContainer.getIndex(hb);
    if (i >= 0) {
      MappeableContainer C = highLowContainer.getContainerAtIndex(i);
      int oldcard = C.getCardinality();
      C = C.add(BufferUtil.lowbits(x));
      getMappeableRoaringArray().setContainerAtIndex(i, C);
      return C.getCardinality() > oldcard;
    } else {
      final MappeableArrayContainer newac = new MappeableArrayContainer();
      getMappeableRoaringArray().insertNewKeyValueAt(-i - 1, hb, newac.add(BufferUtil.lowbits(x)));
      return true;
    }
  }

  /**
   * If present remove the specified integer (effectively, sets its bit value to false)
   *
   * @param x integer value representing the index in a bitmap
   * @return true if the unset bit was already in the bitmap
   */
  public boolean checkedRemove(final int x) {
    final short hb = BufferUtil.highbits(x);
    final int i = highLowContainer.getIndex(hb);
    if (i < 0) {
      return false;
    }
    MappeableContainer C = highLowContainer.getContainerAtIndex(i);
    int oldcard = C.getCardinality();
    C.remove(BufferUtil.lowbits(x));
    int newcard = C.getCardinality();
    if (newcard == oldcard) {
      return false;
    }
    if (newcard > 0) {
      ((MutableRoaringArray) highLowContainer).setContainerAtIndex(i, C);
    } else {
      ((MutableRoaringArray) highLowContainer).removeAtIndex(i);
    }
    return true;
  }

  /**
   * reset to an empty bitmap; result occupies as much space a newly created bitmap.
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
   * Deserialize the bitmap (retrieve from the input stream). The current bitmap is overwritten.
   *
   *  See format specification at https://github.com/RoaringBitmap/RoaringFormatSpec
   *
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserialize(DataInput in) throws IOException {
    try {
      getMappeableRoaringArray().deserialize(in);
    } catch(InvalidRoaringFormat cookie) {
      throw cookie.toIOException();// we convert it to an IOException
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
      MappeableContainer c = highLowContainer.getContainerAtIndex(i);
      c = c.flip(BufferUtil.lowbits(x));
      if (!c.isEmpty()) {
        ((MutableRoaringArray) highLowContainer).setContainerAtIndex(i, c);
      } else {
        ((MutableRoaringArray) highLowContainer).removeAtIndex(i);
      }
    } else {
      final MappeableArrayContainer newac = new MappeableArrayContainer();
      ((MutableRoaringArray) highLowContainer).insertNewKeyValueAt(-i - 1, hb,
          newac.add(BufferUtil.lowbits(x)));
    }
  }



  /**
   * Modifies the current bitmap by complementing the bits in the given range, from rangeStart
   * (inclusive) rangeEnd (exclusive).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   */
  public void flip(final long rangeStart, final long  rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return; // empty range
    }

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
        final MappeableContainer c =
            highLowContainer.getContainerAtIndex(i).inot(containerStart, containerLast + 1);
        if (!c.isEmpty()) {
          getMappeableRoaringArray().setContainerAtIndex(i, c);
        } else {
          getMappeableRoaringArray().removeAtIndex(i);
        }
      } else {
        getMappeableRoaringArray().insertNewKeyValueAt(-i - 1, (short) hb,
            MappeableContainer.rangeOfOnes(containerStart, containerLast + 1));
      }
    }
  }


 /**
   * Modifies the current bitmap by complementing the bits in the given range, from rangeStart
   * (inclusive) rangeEnd (exclusive).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
  public void flip(final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      flip((long) rangeStart, (long) rangeEnd);
    } else {
      // rangeStart being -ve and rangeEnd being positive is not expected)
      // so assume both -ve
      flip(rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL);
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
          iter = MutableRoaringBitmap.this.highLowContainer.getContainerAtIndex(pos)
              .getShortIterator();
          hs = BufferUtil
              .toIntUnsigned(MutableRoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
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
         // todo: implement
        throw new UnsupportedOperationException();
      }

    }.init();
  }

  // call repairAfterLazy on result, eventually
  // important: x2 should not have been computed lazily
  protected void lazyor(final ImmutableRoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      short s1 = highLowContainer.getKeyAtIndex(pos1);
      short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          getMappeableRoaringArray().setContainerAtIndex(pos1, highLowContainer
              .getContainerAtIndex(pos1).lazyIOR(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { // s1 > s2
          getMappeableRoaringArray().insertNewKeyValueAt(pos1, s2,
              x2.highLowContainer.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      getMappeableRoaringArray().appendCopy(x2.highLowContainer, pos2, length2);
    }
  }

  // call repairAfterLazy on result, eventually
  // important: x2 should not have been computed lazily
  // this method is like lazyor except that it will convert
  // the current container to a bitset
  protected void naivelazyor(final ImmutableRoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      short s1 = highLowContainer.getKeyAtIndex(pos1);
      short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          MappeableBitmapContainer c1 = highLowContainer.getContainerAtIndex(pos1)
              .toBitmapContainer();
          getMappeableRoaringArray().setContainerAtIndex(pos1,
              c1.lazyIOR(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { // s1 > s2
          getMappeableRoaringArray().insertNewKeyValueAt(pos1, s2,
              x2.highLowContainer.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      getMappeableRoaringArray().appendCopy(x2.highLowContainer, pos2, length2);
    }
  }




  /**
   * In-place bitwise OR (union) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void or(final ImmutableRoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      short s1 = highLowContainer.getKeyAtIndex(pos1);
      short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          getMappeableRoaringArray().setContainerAtIndex(pos1, highLowContainer
              .getContainerAtIndex(pos1).ior(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { // s1 > s2
          getMappeableRoaringArray().insertNewKeyValueAt(pos1, s2,
              x2.highLowContainer.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      getMappeableRoaringArray().appendCopy(x2.highLowContainer, pos2, length2);
    }
  }



  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    getMappeableRoaringArray().readExternal(in);

  }


  /**
   * If present remove the specified integers (effectively, sets its bit value to false)
   *
   * @param x integer value representing the index in a bitmap
   */
  @Override
  public void remove(final int x) {
    final short hb = BufferUtil.highbits(x);
    final int i = highLowContainer.getIndex(hb);
    if (i < 0) {
      return;
    }
    getMappeableRoaringArray().setContainerAtIndex(i,
        highLowContainer.getContainerAtIndex(i).remove(BufferUtil.lowbits(x)));
    if (highLowContainer.getContainerAtIndex(i).isEmpty()) {
      getMappeableRoaringArray().removeAtIndex(i);
    }
  }

  /**
   * Remove from the current bitmap all integers in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   */
  public void remove(final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return; // empty range
    }
    final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
    final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
    final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
    final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));
    if (hbStart == hbLast) {
      final int i = highLowContainer.getIndex((short) hbStart);
      if (i < 0) {
        return;
      }
      final MappeableContainer c =
          highLowContainer.getContainerAtIndex(i).iremove(lbStart, lbLast + 1);
      if (!c.isEmpty()) {
        ((MutableRoaringArray) highLowContainer).setContainerAtIndex(i, c);
      } else {
        ((MutableRoaringArray) highLowContainer).removeAtIndex(i);
      }
      return;
    }
    int ifirst = highLowContainer.getIndex((short) hbStart);
    int ilast = highLowContainer.getIndex((short) hbLast);
    if (ifirst >= 0) {
      if (lbStart != 0) {
        final MappeableContainer c = highLowContainer.getContainerAtIndex(ifirst).iremove(lbStart,
            BufferUtil.maxLowBitAsInteger() + 1);
        if (!c.isEmpty()) {
          ((MutableRoaringArray) highLowContainer).setContainerAtIndex(ifirst, c);
          ifirst++;
        }
      }
    } else {
      ifirst = -ifirst - 1;
    }
    if (ilast >= 0) {
      if (lbLast != BufferUtil.maxLowBitAsInteger()) {
        final MappeableContainer c =
            highLowContainer.getContainerAtIndex(ilast).iremove(0, lbLast + 1);
        if (!c.isEmpty()) {
          ((MutableRoaringArray) highLowContainer).setContainerAtIndex(ilast, c);
        } else {
          ilast++;
        }
      } else {
        ilast++;
      }
    } else {
      ilast = -ilast - 1;
    }
    ((MutableRoaringArray) highLowContainer).removeIndexRange(ifirst, ilast);
  }


  /**
   * Remove from the current bitmap all integers in [rangeStart,rangeEnd).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
  public void remove(final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      remove((long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    remove(rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL);
  }


  /**
   * Remove run-length encoding even when it is more space efficient
   *
   * @return whether a change was applied
   */
  public boolean removeRunCompression() {
    boolean answer = false;
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      MappeableContainer c = getMappeableRoaringArray().getContainerAtIndex(i);
      if (c instanceof MappeableRunContainer) {
        MappeableContainer mc =
            ((MappeableRunContainer) c).toBitmapOrArrayContainer(c.getCardinality());
        getMappeableRoaringArray().setContainerAtIndex(i, mc);
        answer = true;
      }
    }
    return answer;
  }

  // to be used with lazyor
  protected void repairAfterLazy() {
    for (int k = 0; k < highLowContainer.size(); ++k) {
      MappeableContainer c = highLowContainer.getContainerAtIndex(k);
      ((MutableRoaringArray) highLowContainer).setContainerAtIndex(k, c.repairAfterLazy());
    }
  }


  /**
   * Use a run-length encoding where it is estimated as more space efficient
   *
   * @return whether a change was applied
   */
  public boolean runOptimize() {
    boolean answer = false;
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      MappeableContainer c = getMappeableRoaringArray().getContainerAtIndex(i).runOptimize();
      if (c instanceof MappeableRunContainer) {
        answer = true;
      }
      getMappeableRoaringArray().setContainerAtIndex(i, c);
    }
    return answer;
  }

  /**
   * Convenience method, effectively casts the object to an object of class ImmutableRoaringBitmap.
   *
   *
   * This function is equivalent to :
   *
   * <pre>
   * {@code
   *       (ImmutableRoaringBitmap) bitmap
   * }
   * </pre>
   *
   * Some users would prefer to generate a hard copy of the data. The following
   * code illustrates how to proceed, but note that the resulting copy can be
   * expected to perform significantly worse than the original: the toImmutableRoaringBitmap
   * method is almost free, it uses less memory and it produces a much faster bitmap.
   * <pre>
   * {@code
   *      /////////////
   *      // Code to create a hard copy of MutableRoaringBitmap to an
   *      // ImmutableRoaringBitmap.
   *      // Usage of this code is discouraged because it is expensive
   *      // and it creates a copy that
   *      // suffers from more performance overhead than the original.
   *      /////////////
   *      import org.roaringbitmap.buffer.*;
   *
   *      //...
   *
   *      MutableRoaringBitmap rr = ... // some bitmap
   *      rr.runOptimize(); // can help compression
   *
   *      // we are going to create an immutable copy of rr
   *      ByteBuffer outbb = ByteBuffer.allocate(mrb.serializedSizeInBytes());
   *      mrb.serialize(new DataOutputStream(new OutputStream(){
   *            ByteBuffer mBB;
   *            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
   *            public void close() {}
   *            public void flush() {}
   *            public void write(int b) {
   *                mBB.put((byte) b);}
   *            public void write(byte[] b) {mBB.put(b);}
   *            public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
   *      }.init(outbb)));
   *      outbb.flip();
   *      ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(outbb);
   * }
   * </pre>
   *
   * @return a cast of this object
   */
  public ImmutableRoaringBitmap toImmutableRoaringBitmap() {
    return this;

  }

  /**
   * Recover allocated but unused memory.
   */
  @Override
  public void trim() {
    getMappeableRoaringArray().trim();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    getMappeableRoaringArray().writeExternal(out);
  }



  /**
   * In-place bitwise XOR (symmetric difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void xor(final ImmutableRoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();

    main: if (pos1 < length1 && pos2 < length2) {
      short s1 = highLowContainer.getKeyAtIndex(pos1);
      short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          final MappeableContainer c = highLowContainer.getContainerAtIndex(pos1)
              .ixor(x2.highLowContainer.getContainerAtIndex(pos2));
          if (!c.isEmpty()) {
            this.getMappeableRoaringArray().setContainerAtIndex(pos1, c);
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
        } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { // s1 > s2
          getMappeableRoaringArray().insertNewKeyValueAt(pos1, s2,
              x2.highLowContainer.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      getMappeableRoaringArray().appendCopy(x2.highLowContainer, pos2, length2);
    }
  }

  /**
   * Assume that one wants to store "cardinality" integers in [0, universe_size),
   * this function returns an upper bound on the serialized size in bytes.
   *
   * This function is identical to RoaringBitmap.maximumSerializedSize.
   *
   * @param cardinality maximal cardinality
   * @param universe_size maximal value
   * @return upper bound on the serialized size in bytes of the bitmap
   */
  public static long maximumSerializedSize(int cardinality, int universe_size) {
    return RoaringBitmap.maximumSerializedSize(cardinality, universe_size);
  }
}
