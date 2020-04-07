/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MappeableContainerPointer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.roaringbitmap.RoaringBitmapWriter.writer;
import static org.roaringbitmap.Util.lowbitsAsInteger;



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
 *
 *      //...
 *      DataOutputStream wheretoserialize = ...
 *      rr.runOptimize(); // can help compression
 *      rr.serialize(wheretoserialize);
 * }
 * </pre>
 *
 * Integers are added in unsigned sorted order. That is, they are treated as unsigned integers (see
 * Java 8's Integer.toUnsignedLong function).
 * Up to 4294967296 integers
 * can be stored.
 *
 *
 *
 */


public class RoaringBitmap extends RoaringArray implements Cloneable, Serializable,
        Iterable<Integer>, Externalizable, ImmutableBitmapDataProvider,
        BitmapDataProvider, AppendableStorage<Container> {

  private final class RoaringIntIterator implements PeekableIntIterator {
    private int hs = 0;

    private PeekableCharIterator iter;

    private int pos = 0;

    private RoaringIntIterator() {
      nextContainer();
    }

    @Override
    public PeekableIntIterator clone() {
      try {
        RoaringIntIterator x = (RoaringIntIterator) super.clone();
        if(this.iter != null) {
          x.iter = this.iter.clone();
        }
        return x;
      } catch (CloneNotSupportedException e) {
        return null;// will not happen
      }
    }

    @Override
    public boolean hasNext() {
      return pos < RoaringBitmap.this.size();
    }

    @Override
    public int next() {
      final int x = iter.nextAsInt() | hs;
      if (!iter.hasNext()) {
        ++pos;
        nextContainer();
      }
      return x;
    }

    private void nextContainer() {
      if (pos < RoaringBitmap.this.size()) {
        iter = RoaringBitmap.this.getContainerAtIndex(pos).getCharIterator();
        hs = RoaringBitmap.this.getKeyAtIndex(pos) << 16;
      }
    }

    @Override
    public void advanceIfNeeded(int minval) {
      while (hasNext() && ((hs >>> 16) < (minval >>> 16))) {
        ++pos;
        nextContainer();
      }
      if (hasNext() && ((hs >>> 16) == (minval >>> 16))) {
        iter.advanceIfNeeded(Util.lowbits(minval));
        if (!iter.hasNext()) {
          ++pos;
          nextContainer();
        }
      }
    }

    @Override
    public int peekNext() {
      return (iter.peekNext()) | hs;
    }


  }

  private final class RoaringReverseIntIterator implements IntIterator {

    int hs = 0;

    CharIterator iter;

    int pos = RoaringBitmap.this.size() - 1;

    private RoaringReverseIntIterator() {
      nextContainer();
    }

    @Override
    public IntIterator clone() {
      try {
        RoaringReverseIntIterator clone = (RoaringReverseIntIterator) super.clone();
        if(this.iter != null) {
          clone.iter = this.iter.clone();
        }
        return clone;
      } catch (CloneNotSupportedException e) {
        return null;// will not happen
      }
    }

    @Override
    public boolean hasNext() {
      return pos >= 0;
    }

    @Override
    public int next() {
      final int x = iter.nextAsInt() | hs;
      if (!iter.hasNext()) {
        --pos;
        nextContainer();
      }
      return x;
    }

    private void nextContainer() {
      if (pos >= 0) {
        iter =
            RoaringBitmap.this.getContainerAtIndex(pos).getReverseCharIterator();
        hs = RoaringBitmap.this.getKeyAtIndex(pos) << 16;
      }
    }

  }

  private static final long serialVersionUID = 6L;

  private static void rangeSanityCheck(final long rangeStart, final long rangeEnd) {
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
   * Generate a new bitmap, but with
   * all its values incremented by offset.
   * The parameter offset can be
   * negative. Values that would fall outside
   * of the valid 32-bit range are discarded
   * so that the result can have lower cardinality.
   *
   * @param x source bitmap
   * @param offset increment
   * @return a new bitmap
   */
  public static RoaringBitmap addOffset(final RoaringBitmap x, long offset) {
    // we need "offset" to be a long because we want to support values
    // between -0xFFFFFFFF up to +-0xFFFFFFFF
    long container_offset_long = offset < 0
        ? (offset - (1<<16) + 1)  / (1<<16) : offset / (1 << 16);
    if((container_offset_long <= -(1<<16) ) || (container_offset_long >= (1<<16) )) {
      return new RoaringBitmap(); // it is necessarily going to be empty
    }
    // next cast is necessarily safe, the result is between -0xFFFF and 0xFFFF
    int container_offset = (int) container_offset_long;
    // next case is safe
    int in_container_offset = (int)(offset - container_offset_long * (1L<<16));
    if(in_container_offset == 0) {
      RoaringBitmap answer = new RoaringBitmap();
      for(int pos = 0; pos < x.size(); pos++) {
        int key = (x.getKeyAtIndex(pos));
        key += container_offset;
        answer.append((char)key,
            x.getContainerAtIndex(pos).clone());
      }
      return answer;
    } else {
      RoaringBitmap answer = new RoaringBitmap();
      for(int pos = 0; pos < x.size(); pos++) {
        int key = (x.getKeyAtIndex(pos));
        key += container_offset;
        Container c = x.getContainerAtIndex(pos);
        Container[] offsetted = Util.addOffset(c,
                (char)in_container_offset);
        boolean keyok = (key >= 0) && (key <= 0xFFFF);
        boolean keypok = (key + 1 >= 0) && (key + 1 <= 0xFFFF);
        if( !offsetted[0].isEmpty() && keyok) {
          int current_size = answer.size();
          int lastkey = 0;
          if(current_size > 0) {
            lastkey = (answer.getKeyAtIndex(
                    current_size - 1));
          }
          if((current_size > 0) && (lastkey == key)) {
            Container prev = answer
                    .getContainerAtIndex(current_size - 1);
            Container orresult = prev.ior(offsetted[0]);
            answer.setContainerAtIndex(current_size - 1,
                    orresult);
          } else {
            answer.append((char)key, offsetted[0]);
          }
        }
        if( !offsetted[1].isEmpty()  && keypok) {
          answer.append((char)(key + 1), offsetted[1]);
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
  public static RoaringBitmap add(RoaringBitmap rb, final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return rb.clone(); // empty range
    }


    final int hbStart = (Util.highbits(rangeStart));
    final int lbStart = (Util.lowbits(rangeStart));
    final int hbLast = (Util.highbits(rangeEnd - 1));
    final int lbLast = (Util.lowbits(rangeEnd - 1));

    RoaringBitmap answer = new RoaringBitmap();
    answer.appendCopiesUntil(rb, (char) hbStart);

    if (hbStart == hbLast) {
      final int i = rb.getIndex((char) hbStart);
      final Container c =
          i >= 0 ? rb.getContainerAtIndex(i).add(lbStart, lbLast + 1)
              : Container.rangeOfOnes(lbStart, lbLast + 1);
      answer.append((char) hbStart, c);
      answer.appendCopiesAfter(rb, (char) hbLast);
      return answer;
    }
    int ifirst = rb.getIndex((char) hbStart);
    int ilast = rb.getIndex((char) hbLast);

    {
      final Container c = ifirst >= 0
          ? rb.getContainerAtIndex(ifirst).add(lbStart,
              Util.maxLowBitAsInteger() + 1)
          : Container.rangeOfOnes(lbStart, Util.maxLowBitAsInteger() + 1);
      answer.append((char) hbStart, c);
    }
    for (int hb = hbStart + 1; hb < hbLast; ++hb) {
      Container c = Container.rangeOfOnes(0, Util.maxLowBitAsInteger() + 1);
      answer.append((char) hb, c);
    }
    {
      final Container c =
          ilast >= 0 ? rb.getContainerAtIndex(ilast).add(0, lbLast + 1)
              : Container.rangeOfOnes(0, lbLast + 1);
      answer.append((char) hbLast, c);
    }
    answer.appendCopiesAfter(rb, (char) hbLast);
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
  public static RoaringBitmap add(RoaringBitmap rb, final int rangeStart, final int rangeEnd) {
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
   * If you have more than 2 bitmaps, consider using the FastAggregation class.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   * @see FastAggregation#and(RoaringBitmap...)
   */
  public static RoaringBitmap and(final RoaringBitmap x1, final RoaringBitmap x2) {
    final RoaringBitmap answer = new RoaringBitmap();
    final int length1 = x1.size(), length2 = x2.size();
    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.getKeyAtIndex(pos1);
      final char s2 = x2.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = x1.getContainerAtIndex(pos1);
        final Container c2 = x2.getContainerAtIndex(pos2);
        final Container c = c1.and(c2);
        if (!c.isEmpty()) {
          answer.append(s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        pos1 = x1.advanceUntil(s2, pos1);
      } else { 
        pos2 = x2.advanceUntil(s1, pos2);
      }
    }
    return answer;
  }


  /**
   * Cardinality of Bitwise AND (intersection) operation. The provided bitmaps are *not* modified.
   * This operation is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return as if you did and(x2,x2).getCardinality()
   * @see FastAggregation#and(RoaringBitmap...)
   */
  public static int andCardinality(final RoaringBitmap x1, final RoaringBitmap x2) {
    int answer = 0;
    final int length1 = x1.size(), length2 = x2.size();
    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.getKeyAtIndex(pos1);
      final char s2 = x2.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = x1.getContainerAtIndex(pos1);
        final Container c2 = x2.getContainerAtIndex(pos2);
        // TODO: could be made faster if we did not have to materialize container
        answer += c1.andCardinality(c2);
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        pos1 = x1.advanceUntil(s2, pos1);
      } else { 
        pos2 = x2.advanceUntil(s1, pos2);
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
  public static RoaringBitmap andNot(final RoaringBitmap x1, final RoaringBitmap x2) {
    final RoaringBitmap answer = new RoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.size(), length2 = x2.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.getKeyAtIndex(pos1);
      final char s2 = x2.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = x1.getContainerAtIndex(pos1);
        final Container c2 = x2.getContainerAtIndex(pos2);
        final Container c = c1.andNot(c2);
        if (!c.isEmpty()) {
          answer.append(s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        final int nextPos1 = x1.advanceUntil(s2, pos1);
        answer.appendCopy(x1, pos1, nextPos1);
        pos1 = nextPos1;
      } else { 
        pos2 = x2.advanceUntil(s1, pos2);
      }
    }
    if (pos2 == length2) {
      answer.appendCopy(x1, pos1, length1);
    }
    return answer;
  }

  /**
   * Set all the specified values to true. This can be expected to be slightly
   * faster than calling "add" repeatedly. The provided integers values don't
   * have to be in sorted order, but it may be preferable to sort them from a performance point of
   * view.
   *
   * @param dat set values
   */
  public void add(final int... dat) {
    this.addN(dat, 0, dat.length);
  }

  /**
   * Set the specified values to true, within given boundaries. This can be expected to be slightly
   * faster than calling "add" repeatedly. The provided integers values don't
   * have to be in sorted order, but it may be preferable to sort them from a performance point of
   * view.
   *
   * @param dat set values
   * @param offset from which index the values should be set to true
   * @param n how many values should be set to true
   */
  public void addN(final int[] dat, final int offset, final int n) {
    Container currentcont = null;
    char currenthb = 0;
    int currentcontainerindex = 0;
    int j = 0;
    if(j < n) {
      int val = dat[j + offset];
      currenthb = Util.highbits(val);
      currentcontainerindex = getIndex(currenthb);
      if (currentcontainerindex >= 0) {
        currentcont = getContainerAtIndex(currentcontainerindex);
        Container newcont = currentcont.add(Util.lowbits(val));
        if(newcont != currentcont) {
          setContainerAtIndex(currentcontainerindex, newcont);
          currentcont = newcont;
        }
      } else {
        currentcontainerindex = - currentcontainerindex - 1;
        final ArrayContainer newac = new ArrayContainer();
        currentcont = newac.add(Util.lowbits(val));
        insertNewKeyValueAt(currentcontainerindex, currenthb, currentcont);
      }
      j++;
    }
    for( ; j < n; ++j) {
      int val = dat[j + offset];
      char newhb = Util.highbits(val);
      if(currenthb == newhb) {// easy case
        // this could be quite frequent
        Container newcont = currentcont.add(Util.lowbits(val));
        if(newcont != currentcont) {
          setContainerAtIndex(currentcontainerindex, newcont);
          currentcont = newcont;
        }
      } else {
        currenthb = newhb;
        currentcontainerindex = getIndex(currenthb);
        if (currentcontainerindex >= 0) {
          currentcont = getContainerAtIndex(currentcontainerindex);
          Container newcont = currentcont.add(Util.lowbits(val));
          if(newcont != currentcont) {
            setContainerAtIndex(currentcontainerindex, newcont);
            currentcont = newcont;
          }
        } else {
          currentcontainerindex = - currentcontainerindex - 1;
          final ArrayContainer newac = new ArrayContainer();
          currentcont = newac.add(Util.lowbits(val));
          insertNewKeyValueAt(currentcontainerindex, currenthb, currentcont);
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
  public static RoaringBitmap bitmapOf(final int... dat) {
    final RoaringBitmap ans = new RoaringBitmap();
    ans.add(dat);
    return ans;
  }

  /**
   * Efficiently builds a RoaringBitmap from unordered data
   * @param data unsorted data
   * @return a new bitmap
   */
  public static RoaringBitmap bitmapOfUnordered(final int... data) {
    RoaringBitmapWriter<RoaringBitmap> writer = writer().constantMemory()
            .doPartialRadixSort().get();
    writer.addMany(data);
    writer.flush();
    return writer.getUnderlying();
  }

  /**
   * Complements the bits in the given range, from rangeStart (inclusive) rangeEnd (exclusive). The
   * given bitmap is unchanged.
   *
   * @param bm bitmap being negated
   * @param rangeStart inclusive beginning of range, in [0, 0xffffffff]
   * @param rangeEnd exclusive ending of range, in [0, 0xffffffff + 1]
   * @return a new Bitmap
   */
  public static RoaringBitmap flip(RoaringBitmap bm, final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return bm.clone();
    }
    RoaringBitmap answer = new RoaringBitmap();
    final int hbStart = (Util.highbits(rangeStart));
    final int lbStart = (Util.lowbits(rangeStart));
    final int hbLast = (Util.highbits(rangeEnd - 1));
    final int lbLast = (Util.lowbits(rangeEnd - 1));

    // copy the containers before the active area
    answer.appendCopiesUntil(bm, (char) hbStart);

    for (int hb = hbStart; hb <= hbLast; ++hb) {
      final int containerStart = (hb == hbStart) ? lbStart : 0;
      final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();

      final int i = bm.getIndex((char) hb);
      final int j = answer.getIndex((char) hb);
      assert j < 0;

      if (i >= 0) {
        Container c =
            bm.getContainerAtIndex(i).not(containerStart, containerLast + 1);
        if (!c.isEmpty()) {
          answer.insertNewKeyValueAt(-j - 1, (char) hb, c);
        }

      } else { // *think* the range of ones must never be
        // empty.
        answer.insertNewKeyValueAt(-j - 1, (char) hb,
            Container.rangeOfOnes(containerStart, containerLast + 1));
      }
    }
    // copy the containers after the active area.
    answer.appendCopiesAfter(bm, (char) hbLast);
    return answer;
  }

  /**
   * Complements the bits in the given range, from rangeStart (inclusive) rangeEnd (exclusive). The
   * given bitmap is unchanged.
   *
   * @param rb bitmap being negated
   * @param rangeStart inclusive beginning of range, in [0, 0xffffffff]
   * @param rangeEnd exclusive ending of range, in [0, 0xffffffff + 1]
   * @return a new Bitmap
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
  public static RoaringBitmap flip(RoaringBitmap rb, final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      return flip(rb, (long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    return flip(rb, rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL);
  }





  /**
   * Checks whether the two bitmaps intersect. This can be much faster than calling "and" and
   * checking the cardinality of the result.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return true if they intersect
   */
  public static boolean intersects(final RoaringBitmap x1, final RoaringBitmap x2) {
    final int length1 = x1.size(), length2 = x2.size();
    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.getKeyAtIndex(pos1);
      final char s2 = x2.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = x1.getContainerAtIndex(pos1);
        final Container c2 = x2.getContainerAtIndex(pos2);
        if (c1.intersects(c2)) {
          return true;
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        pos1 = x1.advanceUntil(s2, pos1);
      } else { 
        pos2 = x2.advanceUntil(s1, pos2);
      }
    }
    return false;
  }


  // important: inputs should not have been computed lazily
  protected static RoaringBitmap lazyor(final RoaringBitmap x1, final RoaringBitmap x2) {
    final RoaringBitmap answer = new RoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.size(), length2 = x2.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = x1.getKeyAtIndex(pos1);
      char s2 = x2.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          answer.append(s1, x1.getContainerAtIndex(pos1)
              .lazyOR(x2.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.getKeyAtIndex(pos1);
          s2 = x2.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          answer.appendCopy(x1, pos1);
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.getKeyAtIndex(pos1);
        } else { 
          answer.appendCopy(x2, pos2);
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      answer.appendCopy(x2, pos2, length2);
    } else if (pos2 == length2) {
      answer.appendCopy(x1, pos1, length1);
    }
    return answer;
  }

  // important: inputs should not be reused
  protected static RoaringBitmap lazyorfromlazyinputs(final RoaringBitmap x1,
      final RoaringBitmap x2) {
    final RoaringBitmap answer = new RoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.size(), length2 = x2.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = x1.getKeyAtIndex(pos1);
      char s2 = x2.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          Container c1 = x1.getContainerAtIndex(pos1);
          Container c2 = x2.getContainerAtIndex(pos2);
          if ((c2 instanceof BitmapContainer) && (!(c1 instanceof BitmapContainer))) {
            Container tmp = c1;
            c1 = c2;
            c2 = tmp;
          }
          answer.append(s1, c1.lazyIOR(c2));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.getKeyAtIndex(pos1);
          s2 = x2.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          Container c1 = x1.getContainerAtIndex(pos1);
          answer.append(s1, c1);
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.getKeyAtIndex(pos1);
        } else { 
          Container c2 = x2.getContainerAtIndex(pos2);
          answer.append(s2,c2);
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      answer.append(x2, pos2, length2);
    } else if (pos2 == length2) {
      answer.append(x1, pos1, length1);
    }
    return answer;
  }


  /**
   * Compute overall OR between bitmaps.
   *
   * (Effectively calls {@link FastAggregation#or})
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap or(Iterator<? extends RoaringBitmap> bitmaps) {
    return FastAggregation.or(bitmaps);
  }

  /**
   * Compute overall OR between bitmaps.
   *
   * (Effectively calls {@link FastAggregation#or})
   *
   *
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static RoaringBitmap or(RoaringBitmap... bitmaps) {
    return FastAggregation.or(bitmaps);
  }

  /**
   * Bitwise OR (union) operation. The provided bitmaps are *not* modified. This operation is
   * thread-safe as long as the provided bitmaps remain unchanged.
   *
   * If you have more than 2 bitmaps, consider using the FastAggregation class.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   * @see FastAggregation#or(RoaringBitmap...)
   * @see FastAggregation#horizontal_or(RoaringBitmap...)
   */
  public static RoaringBitmap or(final RoaringBitmap x1, final RoaringBitmap x2) {
    final RoaringBitmap answer = new RoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.size(), length2 = x2.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = x1.getKeyAtIndex(pos1);
      char s2 = x2.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          answer.append(s1, x1.getContainerAtIndex(pos1)
              .or(x2.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.getKeyAtIndex(pos1);
          s2 = x2.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          answer.appendCopy(x1, pos1);
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.getKeyAtIndex(pos1);
        } else { 
          answer.appendCopy(x2, pos2);
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      answer.appendCopy(x2, pos2, length2);
    } else if (pos2 == length2) {
      answer.appendCopy(x1, pos1, length1);
    }
    return answer;
  }

  /**
   * Cardinality of the bitwise OR (union) operation. The provided bitmaps are *not* modified. This
   * operation is thread-safe as long as the provided bitmaps remain unchanged.
   *
   * If you have more than 2 bitmaps, consider using the FastAggregation class.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return cardinality of the union
   * @see FastAggregation#or(RoaringBitmap...)
   * @see FastAggregation#horizontal_or(RoaringBitmap...)
   */
  public static int orCardinality(final RoaringBitmap x1, final RoaringBitmap x2) {
    // we use the fact that the cardinality of the bitmaps is known so that
    // the union is just the total cardinality minus the intersection
    return x1.getCardinality() + x2.getCardinality() - andCardinality(x1, x2);
  }

  /**
   * Cardinality of the bitwise XOR (symmetric difference) operation.
   * The provided bitmaps are *not* modified. This operation is thread-safe
   * as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return cardinality of the symmetric difference
   */
  public static int xorCardinality(final RoaringBitmap x1, final RoaringBitmap x2) {
    return x1.getCardinality() + x2.getCardinality() - 2 * andCardinality(x1, x2);
  }

  /**
   * Cardinality of the bitwise ANDNOT (left difference) operation.
   * The provided bitmaps are *not* modified. This operation is thread-safe
   * as long as the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return cardinality of the left difference
   */
  public static int andNotCardinality(final RoaringBitmap x1, final RoaringBitmap x2) {
    return x1.getCardinality() - andCardinality(x1, x2);
  }

  /**
   * Generate a new bitmap with all integers in [rangeStart,rangeEnd) removed.
   *
   * @param rb initial bitmap (will not be modified)
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new bitmap
   */
  public static RoaringBitmap remove(RoaringBitmap rb, final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return rb.clone(); // empty range
    }


    final int hbStart = (Util.highbits(rangeStart));
    final int lbStart = (Util.lowbits(rangeStart));
    final int hbLast = (Util.highbits(rangeEnd - 1));
    final int lbLast = (Util.lowbits(rangeEnd - 1));
    RoaringBitmap answer = new RoaringBitmap();
    answer.appendCopiesUntil(rb, (char) hbStart);

    if (hbStart == hbLast) {
      final int i = rb.getIndex((char) hbStart);
      if (i >= 0) {
        final Container c = rb.getContainerAtIndex(i).remove(lbStart, lbLast + 1);
        if (!c.isEmpty()) {
          answer.append((char) hbStart, c);
        }
      }
      answer.appendCopiesAfter(rb, (char) hbLast);
      return answer;
    }
    int ifirst = rb.getIndex((char) hbStart);
    int ilast = rb.getIndex((char) hbLast);
    if ((ifirst >= 0) && (lbStart != 0)) {
      final Container c = rb.getContainerAtIndex(ifirst).remove(lbStart,
          Util.maxLowBitAsInteger() + 1);
      if (!c.isEmpty()) {
        answer.append((char) hbStart, c);
      }
    }
    if ((ilast >= 0) && (lbLast != Util.maxLowBitAsInteger())) {
      final Container c = rb.getContainerAtIndex(ilast).remove(0, lbLast + 1);
      if (!c.isEmpty()) {
        answer.append((char) hbLast, c);
      }
    }
    answer.appendCopiesAfter(rb, (char) hbLast);
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
  public static RoaringBitmap remove(RoaringBitmap rb, final int rangeStart, final int rangeEnd) {
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
   * If you have more than 2 bitmaps, consider using the FastAggregation class.
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
    final int length1 = x1.size(), length2 = x2.size();

    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = x1.getKeyAtIndex(pos1);
      char s2 = x2.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          final Container c = x1.getContainerAtIndex(pos1)
              .xor(x2.getContainerAtIndex(pos2));
          if (!c.isEmpty()) {
            answer.append(s1, c);
          }
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.getKeyAtIndex(pos1);
          s2 = x2.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          answer.appendCopy(x1, pos1);
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.getKeyAtIndex(pos1);
        } else { 
          answer.appendCopy(x2, pos2);
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      answer.appendCopy(x2, pos2, length2);
    } else if (pos2 == length2) {
      answer.appendCopy(x1, pos1, length1);
    }

    return answer;
  }

  /**
   * Create an empty bitmap
   */
  public RoaringBitmap() {
    super();
  }

  RoaringBitmap(RoaringArray array) {
    this(array.keys, array.values, array.size);
  }

  RoaringBitmap(char[] keys, Container[] values, int cardinality) {
    super(keys, values, cardinality);
  }

  RoaringBitmap(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * Create a RoaringBitmap from a MutableRoaringBitmap or ImmutableRoaringBitmap. The source is not
   * modified.
   *
   * @param rb the original bitmap
   */
  public RoaringBitmap(ImmutableRoaringBitmap rb) {
    super();
    MappeableContainerPointer cp = rb.getContainerPointer();
    while (cp.getContainer() != null) {
      append(cp.key(), cp.getContainer().toContainer());
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
    final char hb = Util.highbits(x);
    final int i = getIndex(hb);
    if (i >= 0) {
      setContainerAtIndex(i, getContainerAtIndex(i).add(Util.lowbits(x)));
    } else {
      final ArrayContainer newac = new ArrayContainer();
      insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
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

    final int hbStart = (Util.highbits(rangeStart));
    final int lbStart = (Util.lowbits(rangeStart));
    final int hbLast = (Util.highbits(rangeEnd - 1));
    final int lbLast = (Util.lowbits(rangeEnd - 1));
    for (int hb = hbStart; hb <= hbLast; ++hb) {

      // first container may contain partial range
      final int containerStart = (hb == hbStart) ? lbStart : 0;
      // last container may contain partial range
      final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();
      final int i = getIndex((char) hb);

      if (i >= 0) {
        final Container c =
            getContainerAtIndex(i).iadd(containerStart, containerLast + 1);
        setContainerAtIndex(i, c);
      } else {
        insertNewKeyValueAt(-i - 1, (char) hb,
            Container.rangeOfOnes(containerStart, containerLast + 1));
      }
    }
  }

  /**
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
   * Checks if the range intersects with the bitmap.
   * @param minimum the inclusive unsigned lower bound of the range
   * @param supremum the exclusive unsigned upper bound of the range
   * @return whether the bitmap intersects with the range
   */
  public boolean intersects(long minimum, long supremum) {
    rangeSanityCheck(minimum, supremum);
    int minKey = (int)(minimum >>> 16);
    int supKey = (int)(supremum >>> 16);
    int length = size;
    char[] keys = this.keys;
    int offset = lowbitsAsInteger(minimum);
    int limit = lowbitsAsInteger(supremum);
    int index = Util.unsignedBinarySearch(keys, 0, length, (char)minKey);
    int pos = index >= 0 ? index : -index - 1;
    if (pos < length && supKey == (keys[pos])) {
      if (supKey > minKey) {
        offset = 0;
      }
      return getContainerAtIndex(pos).intersects(offset, limit);
    }
    while (pos < length && supKey > (keys[pos])) {
      Container container = getContainerAtIndex(pos);
      if (container.intersects(offset, 1 << 16)) {
        return true;
      }
      offset = 0;
      ++pos;
    }
    return pos < length && supKey == keys[pos]
            && getContainerAtIndex(pos)
            .intersects(offset, limit);
  }


  /**
   * In-place bitwise AND (intersection) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void and(final RoaringBitmap x2) {
    int pos1 = 0, pos2 = 0, intersectionSize = 0;
    final int length1 = size(), length2 = x2.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = getKeyAtIndex(pos1);
      final char s2 = x2.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = getContainerAtIndex(pos1);
        final Container c2 = x2.getContainerAtIndex(pos2);
        final Container c = c1.iand(c2);
        if (!c.isEmpty()) {
          replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        pos1 = advanceUntil(s2, pos1);
      } else { 
        pos2 = x2.advanceUntil(s1, pos2);
      }
    }
    resize(intersectionSize);
  }


  /**
   * Computes AND between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   */
  public static RoaringBitmap and(final Iterator<? extends RoaringBitmap> bitmaps,
      final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);

    Iterator<RoaringBitmap> bitmapsIterator;
    bitmapsIterator = selectRangeWithoutCopy(bitmaps, rangeStart, rangeEnd);
    return FastAggregation.and(bitmapsIterator);
  }

  /*
   *     In testing, original int-range code failed an assertion with some negative ranges
   *     so presumably nobody relies on negative ranges. rangeEnd=0 also failed.
   */

  /**
   * Computes AND between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   * @deprecated use the version where longs specify the range. Negative range end are illegal.
   */
  @Deprecated
  public static RoaringBitmap and(final Iterator<? extends RoaringBitmap> bitmaps,
      final int rangeStart, final int rangeEnd) {
    return and(bitmaps, (long) rangeStart, (long) rangeEnd);
  }




  /**
   * In-place bitwise ANDNOT (difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void andNot(final RoaringBitmap x2) {
    int pos1 = 0, pos2 = 0, intersectionSize = 0;
    final int length1 = size(), length2 = x2.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = getKeyAtIndex(pos1);
      final char s2 = x2.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = getContainerAtIndex(pos1);
        final Container c2 = x2.getContainerAtIndex(pos2);
        final Container c = c1.iandNot(c2);
        if (!c.isEmpty()) {
          replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        if (pos1 != intersectionSize) {
          final Container c1 = getContainerAtIndex(pos1);
          replaceKeyAndContainerAtIndex(intersectionSize, s1, c1);
        }
        ++intersectionSize;
        ++pos1;
      } else { 
        pos2 = x2.advanceUntil(s1, pos2);
      }
    }
    if (pos1 < length1) {
      copyRange(pos1, length1, intersectionSize);
      intersectionSize += length1 - pos1;
    }
    resize(intersectionSize);
  }


  /**
   * Bitwise ANDNOT (difference) operation for the given range, rangeStart (inclusive) and rangeEnd
   * (exclusive). The provided bitmaps are *not* modified. This operation is thread-safe as long as
   * the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @param rangeStart starting point of the range (inclusive)
   * @param rangeEnd end point of the range (exclusive)
   * @return result of the operation
   */
  public static RoaringBitmap andNot(final RoaringBitmap x1, final RoaringBitmap x2,
      long rangeStart, long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);

    RoaringBitmap rb1 = selectRangeWithoutCopy(x1, rangeStart, rangeEnd);
    RoaringBitmap rb2 = selectRangeWithoutCopy(x2, rangeStart, rangeEnd);
    return andNot(rb1, rb2);
  }

  /**
   * Bitwise ANDNOT (difference) operation for the given range, rangeStart (inclusive) and rangeEnd
   * (exclusive). The provided bitmaps are *not* modified. This operation is thread-safe as long as
   * the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @param rangeStart starting point of the range (inclusive)
   * @param rangeEnd end point of the range (exclusive)
   * @return result of the operation
   *
   * @deprecated use the version where longs specify the range. Negative values for range
   *     endpoints are not allowed.
   */
  @Deprecated
  public static RoaringBitmap andNot(final RoaringBitmap x1, final RoaringBitmap x2,
          final int rangeStart, final int rangeEnd) {
    return andNot(x1, x2, (long) rangeStart, (long) rangeEnd);
  }

  /*
   * Handle the orNot operations for the remaining containers in the Bitmap.
   * This is done by iterating over the remaining containers while treating the holes
   * (from s2 till lastKey)
   *
   * For each iteration Two cases here:
   * 1. either we have a existing container. In this case, we replace it by a full.
   * 2. or there is no container. In this case, we insert a full one.
   *
   * Note that, at this stage, all the other bitmap containers were treated.
   * That's why we only have to handle the .
   */

  private static char orNotHandleRemainingSelfContainers(
          RoaringBitmap src, RoaringBitmap dest, int  pos1, int length1, char s2,
          char lastKey, int lastSize, boolean inplace) {
    final int insertionIncrement = inplace ? 1 : 0;
    int destPos = inplace ? pos1 : dest.size();

    while (pos1 < length1 && s2 - lastKey <= 0) { // s2 <= lastKey
      final char s1 = src.getKeyAtIndex(pos1);
      final int containerLast = (s2 == lastKey) ? lastSize : Util.maxLowBitAsInteger();
      Container c2 = Container.rangeOfOnes(0, containerLast + 1);

      if (s1 == s2) {
        final Container c1 = src.getContainerAtIndex(pos1);
        // If we re not at the last container, just use the full container.
        // Otherwise, compute an in-place or.
        final Container c = (s2 == lastKey) ? (inplace ? c1.ior(c2) : c1.or(c2)) : c2;
        if (destPos < dest.size()) {
          dest.replaceKeyAndContainerAtIndex(destPos, s1, c);
        } else {
          dest.insertNewKeyValueAt(destPos, s1, c);
        }

        pos1++;
        s2++;
        destPos++;
      } else if (s1 - s2 > 0) { 
        dest.insertNewKeyValueAt(destPos, s2, c2);
        pos1 += insertionIncrement;
        length1 += insertionIncrement;
        s2++;
        destPos++;
      } else { 
        throw new IllegalStateException("This is a bug. Please report to github");
      }
    }
    return s2;
  }

  /*
   * Handle the orNot operations for the remaining containers in the other Bitmap.
   * Two cases here:
   * 1. either we have a hole. In this case, a full container should be appended.
   * 2. or we have a container. an inplace orNot is applied and the result is appended.
   *
   * Note that, at this stage, all the own containers were treated.
   * That's why we only have to append.
   */
  private static char orNotHandleRemainingOtherContainers(
          final RoaringBitmap other, final RoaringBitmap dest, int pos2,
          int length2, char s2, char lastKey, int lastSize) {
    while (pos2 < length2 && s2 - lastKey <= 0) { // s2 <= lastKey
      final int containerLast = (s2 == lastKey) ? lastSize : Util.maxLowBitAsInteger();
      if (s2 == other.getKeyAtIndex(pos2)) {
        final Container c2 = other.getContainerAtIndex(pos2);
        Container c = new RunContainer().iorNot(c2, containerLast + 1);
        dest.append(s2, c);
        pos2++;
      } else {
        dest.append(s2, Container.rangeOfOnes(0, containerLast + 1));
      }
      s2++;
    }
    return s2;
  }

  /*
   * Handle the remaining holes.
   * A full container should be appended for each key.
   */
  private static void orNotHandleRemainingHoles(
          RoaringBitmap dest, char s2, char lastKey, int lastSize) {
    while (s2 < lastKey) { 
      dest.append(s2, RunContainer.full());
      s2++;
    }
    if (s2 == lastKey) {
      dest.append(s2, Container.rangeOfOnes(0, lastSize + 1));
    }
  }

  /**
   * In-place bitwise ORNOT operation. The current bitmap is modified.
   *
   * @param other the other bitmap
   * @param rangeEnd end point of the range (exclusive)
   */
  public void orNot(final RoaringBitmap other, long rangeEnd) {
    rangeSanityCheck(0, rangeEnd);

    int pos1 = 0, pos2 = 0;
    int length1 = size();
    final int length2 = other.size();

    final char lastKey = Util.highbits(rangeEnd - 1);
    final int lastSize = (Util.lowbits(rangeEnd - 1));

    char s2 = 0;
    boolean loopedAtleastOnce = (length1 > 0 && length2 > 0
            && (char) 0 - lastKey <= 0);
    while (pos1 < length1 && pos2 < length2
            && s2 - lastKey <= 0) { // s2 <= lastKey
      final char s1 = getKeyAtIndex(pos1);
      final int containerLast = (s2 == lastKey) ? lastSize : Util.maxLowBitAsInteger();

      if (s1 == s2) {
        final Container c1 = getContainerAtIndex(pos1);
        if (s2 == other.getKeyAtIndex(pos2)) {
          final Container c2 = other.getContainerAtIndex(pos2);
          final Container c = c1.iorNot(c2, containerLast + 1);
          replaceKeyAndContainerAtIndex(pos1, s1, c);
          pos2++;
        } else {
          replaceKeyAndContainerAtIndex(pos1, s1,
                  Container.rangeOfOnes(0, containerLast + 1));
        }
        pos1++;
        s2++;
      } else if (s1 - s2 > 0) { 
        if (s2 == other.getKeyAtIndex(pos2)) {
          final Container c2 = other.getContainerAtIndex(pos2);
          Container c = new RunContainer().iorNot(c2, containerLast + 1);
          insertNewKeyValueAt(pos1, s2, c);
          pos2++;
        } else {
          insertNewKeyValueAt(pos1, s2,
                  Container.rangeOfOnes(0, containerLast + 1));
        }
        // Move forward because we inserted an element in the container.
        pos1++;
        length1++;
        s2++;
      } else { 
        throw new IllegalStateException("This is a bug. Please report to github");
      }
    }

    // s2 == 0  means either that the bitmap is empty or that we wrapped around.
    // In both cases, we want to stop
    boolean loopHasWrapped = loopedAtleastOnce && (s2 == 0);
    if (!loopHasWrapped && s2 - lastKey <= 0) { // s2 <= lastKey
      char newS2;
      if (pos1 < length1) {
        //all the "other" arrays were treated. Handle self containers.
        newS2 = orNotHandleRemainingSelfContainers(this, this, pos1, length1, s2,
                lastKey, lastSize, true);

      } else {
        // all the original arrays were treated.
        // We just need to iterate on the rest of the other arrays while handling holes.
        newS2 = orNotHandleRemainingOtherContainers(other, this, pos2, length2, s2,
                lastKey, lastSize);
      }
      // Check that we didn't wrap around
      if (newS2 >= s2) {
        orNotHandleRemainingHoles(this, newS2, lastKey, lastSize);
      }
    }
  }


  private static RoaringBitmap doOrNot(
          final RoaringBitmap rb1, final RoaringBitmap rb2, long rangeEnd) {
    final RoaringBitmap answer = new RoaringBitmap();

    int pos1 = 0, pos2 = 0;
    int length1 = rb1.size();
    final int length2 = rb2.size();

    final char lastKey = Util.highbits(rangeEnd - 1);
    final int lastSize = (Util.lowbits(rangeEnd - 1));

    char s2 = 0;
    boolean loopedAtleastOnce = (length1 > 0 && length2 > 0
            && (char) 0 - lastKey <= 0);
    while (pos1 < length1 && pos2 < length2
            && s2 - lastKey <= 0) { // s2 <= lastKey
      final char s1 = rb1.getKeyAtIndex(pos1);
      final int containerLast = (s2 == lastKey) ? lastSize : Util.maxLowBitAsInteger();

      if (s1 == s2) {
        final Container c1 = rb1.getContainerAtIndex(pos1);
        if (s2 == rb2.getKeyAtIndex(pos2)) {
          final Container c2 = rb2.getContainerAtIndex(pos2);
          final Container c = c1.orNot(c2, containerLast + 1);
          answer.append(s1, c);
          pos2++;
        } else {
          answer.append(s1,
                  Container.rangeOfOnes(0, containerLast + 1));
        }
        pos1++;
        s2++;
      } else if (s1 - s2 > 0) { 
        if (s2 == rb2.getKeyAtIndex(pos2)) {
          final Container c2 = rb2.getContainerAtIndex(pos2);
          Container c = new RunContainer().orNot(c2, containerLast + 1);
          answer.append(s2, c);
          pos2++;
        } else {
          answer.append(s2,
                  Container.rangeOfOnes(0, containerLast + 1));
        }
        s2++;
      } else { 
        throw new IllegalStateException("This is a bug. Please report to github");
      }
    }

    boolean loopHasWrapped = loopedAtleastOnce && (s2 == 0);
    if (!loopHasWrapped && s2 - lastKey <= 0) { // s2 <= lastKey
      char newS2;
      if (pos1 < length1) {
        //all the "other" arrays were treated. Handle self containers.
        answer.extendArray(lastKey + 1);
        newS2 = orNotHandleRemainingSelfContainers(rb1, answer, pos1, length1, s2,
                lastKey, lastSize, false);
      } else {
        // all the original arrays were treated.
        // We just need to iterate on the rest of the other arrays while handling holes.
        newS2 = orNotHandleRemainingOtherContainers(rb2, answer, pos2, length2, s2,
                lastKey, lastSize);
      }
      // Check that we didnt wrap around
      if (newS2 >= s2) {
        orNotHandleRemainingHoles(answer, newS2, lastKey, lastSize);
      }

    }
    return answer;
  }

  /**
   * Bitwise ORNOT operation for the given range, rangeStart (inclusive) and rangeEnd
   * (exclusive).
   * The provided bitmaps are *not* modified. This operation is thread-safe as long as
   * the provided bitmaps remain unchanged.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @param rangeEnd end point of the range (exclusive)
   * @return result of the operation
   */
  public static RoaringBitmap orNot(
          final RoaringBitmap x1, final RoaringBitmap x2, long rangeEnd) {
    rangeSanityCheck(0, rangeEnd);

    final RoaringBitmap rb1 = selectRangeWithoutCopy(x1, 0, rangeEnd);
    final RoaringBitmap rb2 = selectRangeWithoutCopy(x2, 0, rangeEnd);

    return doOrNot(rb1, rb2, rangeEnd);
  }



  /**
   * Add the value to the container (set the value to "true"), whether it already appears or not.
   *
   * @param x integer value
   * @return true if the added int wasn't already contained in the bitmap. False otherwise.
   */
  public boolean checkedAdd(final int x) {
    final char hb = Util.highbits(x);
    final int i = getIndex(hb);
    if (i >= 0) {
      Container c = getContainerAtIndex(i);
      int oldCard = c.getCardinality();
      // we need to keep the newContainer if a switch between containers type
      // occur, in order to get the new cardinality
      Container newCont = c.add(Util.lowbits(x));
      setContainerAtIndex(i, newCont);
      if (newCont.getCardinality() > oldCard) {
        return true;
      }
    } else {
      final ArrayContainer newac = new ArrayContainer();
      insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
      return true;
    }
    return false;
  }

  /**
   * If present remove the specified integer (effectively, sets its bit value to false)
   *
   * @param x integer value representing the index in a bitmap
   * @return true if the unset bit was already in the bitmap
   */
  public boolean checkedRemove(final int x) {
    final char hb = Util.highbits(x);
    final int i = getIndex(hb);
    if (i < 0) {
      return false;
    }
    Container C = getContainerAtIndex(i);
    int oldcard = C.getCardinality();
    C.remove(Util.lowbits(x));
    int newcard = C.getCardinality();
    if (newcard == oldcard) {
      return false;
    }
    if (newcard > 0) {
      setContainerAtIndex(i, C);
    } else {
      removeAtIndex(i);
    }
    return true;
  }

  /**
   * reset to an empty bitmap; result occupies as much space a newly created bitmap.
   */
  public void clear() {
    super.clear(); // lose references
  }

  @Override
  public RoaringBitmap clone() {
    char[] keys = Arrays.copyOf(this.keys, this.size);
    Container[] values = Arrays.copyOf(this.values, this.size);
    for (int k = 0; k < this.size; ++k) {
      values[k] = values[k].clone();
    }
    return new RoaringBitmap(keys, values, this.size);
  }

  /**
   * Checks whether the value is included, which is equivalent to checking if the corresponding bit
   * is set (get in BitSet class).
   *
   * @param x integer value
   * @return whether the integer value is included.
   */
  @Override
  public boolean contains(final int x) {
    final char hb = Util.highbits(x);
    final Container c = getContainer(hb);
    return c != null && c.contains(Util.lowbits(x));
  }

  /**
   * Checks if the bitmap contains the range.
   * @param minimum the inclusive lower bound of the range
   * @param supremum the exclusive upper bound of the range
   * @return whether the bitmap contains the range
   */
  public boolean contains(long minimum, long supremum) {
    rangeSanityCheck(minimum, supremum);
    char firstKey = Util.highbits(minimum);
    char lastKey = Util.highbits(supremum);
    int span = (lastKey) - (firstKey);
    int len = size;
    if (len < span) {
      return false;
    }
    int begin = getIndex(firstKey);
    int end = getIndex(lastKey);
    end = end < 0 ? -end -1 : end;
    if (begin < 0 || end - begin != span) {
      return false;
    }

    int min = (char)minimum;
    int sup = (char)supremum;
    if (firstKey == lastKey) {
      return getContainerAtIndex(begin).contains(min, sup);
    }
    if (!getContainerAtIndex(begin).contains(min, 1 << 16)) {
      return false;
    }
    if (end < len && !getContainerAtIndex(end).contains(0, sup)) {
      return false;
    }
    for (int i = begin + 1; i < end; ++i) {
      if (getContainerAtIndex(i).getCardinality() != 1 << 16) {
        return false;
      }
    }
    return true;
  }


  /**
   * Deserialize (retrieve) this bitmap. See format specification at
   * https://github.com/RoaringBitmap/RoaringFormatSpec
   *
   * The current bitmap is overwritten.
   *
   * @param in the DataInput stream
   * @param buffer The buffer gets overwritten with data during deserialization. You can pass a NULL
   *        reference as a buffer. A buffer containing at least 8192 bytes might be ideal for
   *        performance. It is recommended to reuse the buffer between calls to deserialize (in a
   *        single-threaded context) for best performance.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserialize(DataInput in, byte[] buffer) throws IOException {
    try {
      this.deserializeContent(in, buffer);
    } catch(InvalidRoaringFormat cookie) {
      throw cookie.toIOException();// we convert it to an IOException
    }
  }

  /**
   * Deserialize (retrieve) this bitmap.
   * See format specification at https://github.com/RoaringBitmap/RoaringFormatSpec
   *
   * The current bitmap is overwritten.
   *
   * @param in the DataInput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserialize(DataInput in) throws IOException {
    try {
      this.deserializeContent(in);
    } catch(InvalidRoaringFormat cookie) {
      throw cookie.toIOException();// we convert it to an IOException
    }
  }

  /**
   * Deserialize (retrieve) this bitmap.
   * See format specification at https://github.com/RoaringBitmap/RoaringFormatSpec
   *
   * The current bitmap is overwritten.
   *
   * It is not necessary that limit() on the input ByteBuffer indicates the end of the serialized
   * data.
   *
   * After loading this RoaringBitmap, you can advance to the rest of the data (if there
   * is more) by setting bbf.position(bbf.position() + bitmap.serializedSizeInBytes());
   *
   * Note that the input ByteBuffer is effectively copied (with the slice operation) so you should
   * expect the provided ByteBuffer to remain unchanged.
   *
   * @param bbf the byte buffer (can be mapped, direct, array backed etc.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void deserialize(ByteBuffer bbf) throws IOException {
    try {
      super.deserializeContent(bbf);
    } catch(InvalidRoaringFormat cookie) {
      throw cookie.toIOException();// we convert it to an IOException
    }
  }

  /**
   * Returns true if the other bitmap has no more than tolerance bits
   * differing from this bitmap. The other may be transformed into a bitmap equal
   * to this bitmap in no more than tolerance bit flips if this method returns true.
   *
   * @param other the bitmap to compare to
   * @param tolerance the maximum number of bits that may differ
   * @return true if the number of differing bits is smaller than tolerance
   */
  public boolean isHammingSimilar(RoaringBitmap other, int tolerance) {
    final int size1 = size();
    final int size2 = other.size();
    int pos1 = 0;
    int pos2 = 0;
    int budget = tolerance;
    while(budget >= 0 && pos1 < size1 && pos2 < size2) {
      final char key1 = this.getKeyAtIndex(pos1);
      final char key2 = other.getKeyAtIndex(pos2);
      Container left = getContainerAtIndex(pos1);
      Container right = other.getContainerAtIndex(pos2);
      if(key1 == key2) {
        budget -= left.xorCardinality(right);
        ++pos1;
        ++pos2;
      } else if(key1 < key2) {
        budget -= left.getCardinality();
        ++pos1;
      } else {
        budget -= right.getCardinality();
        ++pos2;
      }
    }
    while(budget >= 0 && pos1 < size1) {
      Container container = getContainerAtIndex(pos1++);
      budget -= container.getCardinality();
    }
    while(budget >= 0 && pos2 < size2) {
      Container container = other.getContainerAtIndex(pos2++);
      budget -= container.getCardinality();
    }
    return budget >= 0;
  }

  /**
   * Add the value if it is not already present, otherwise remove it.
   *
   * @param x integer value
   */
  public void flip(final int x) {
    final char hb = Util.highbits(x);
    final int i = getIndex(hb);
    if (i >= 0) {
      Container c = getContainerAtIndex(i).flip(Util.lowbits(x));
      if (!c.isEmpty()) {
        setContainerAtIndex(i, c);
      } else {
        removeAtIndex(i);
      }
    } else {
      final ArrayContainer newac = new ArrayContainer();
      insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
    }
  }

  /**
   * Modifies the current bitmap by complementing the bits in the given range, from rangeStart
   * (inclusive) rangeEnd (exclusive).
   *
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   */
  public void flip(final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      return; // empty range
    }

    final int hbStart = (Util.highbits(rangeStart));
    final int lbStart = (Util.lowbits(rangeStart));
    final int hbLast = (Util.highbits(rangeEnd - 1));
    final int lbLast = (Util.lowbits(rangeEnd - 1));

    // TODO:this can be accelerated considerably
    for (int hb = hbStart; hb <= hbLast; ++hb) {
      // first container may contain partial range
      final int containerStart = (hb == hbStart) ? lbStart : 0;
      // last container may contain partial range
      final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();
      final int i = getIndex((char) hb);

      if (i >= 0) {
        final Container c =
            getContainerAtIndex(i).inot(containerStart, containerLast + 1);
        if (!c.isEmpty()) {
          setContainerAtIndex(i, c);
        } else {
          removeAtIndex(i);
        }
      } else {
        insertNewKeyValueAt(-i - 1, (char) hb,
            Container.rangeOfOnes(containerStart, containerLast + 1));
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
   * Returns the number of distinct integers added to the bitmap (e.g., number of bits set).
   *
   * @return the cardinality
   */
  @Override
  public long getLongCardinality() {
    long size = 0;
    for (int i = 0; i < this.size(); i++) {
      size += this.getContainerAtIndex(i).getCardinality();
    }
    return size;
  }

  @Override
  public int getCardinality() {
    return (int) getLongCardinality();
  }

  @Override
  public void forEach(IntConsumer ic) {
    for (int i = 0; i < this.size(); i++) {
      this.getContainerAtIndex(i).forEach(this.keys[i], ic);
    }
  }


  /**
   *
   * For better performance, consider the Use the {@link #forEach forEach} method.
   *
   * @return a custom iterator over set bits, the bits are traversed in ascending sorted order
   */
  @Override
  public PeekableIntIterator getIntIterator() {
    return new RoaringIntIterator();
  }

  /**
   * @return a custom iterator over set bits, the bits are traversed in descending sorted order
   */
  @Override
  public IntIterator getReverseIntIterator() {
    return new RoaringReverseIntIterator();
  }


  @Override
  public RoaringBatchIterator getBatchIterator() {
    return new RoaringBatchIterator(this);
  }

  /**
   * Estimate of the memory usage of this data structure. This can be expected to be within 1% of
   * the true memory usage in common usage scenarios.
   * If exact measures are needed, we recommend using dedicated libraries
   * such as ehcache-sizeofengine.
   *
   * In adversarial cases, this estimate may be 10x the actual memory usage. For example, if
   * you insert a single random value in a bitmap, then over a 100 bytes may be used by the JVM
   * whereas this function may return an estimate of 32 bytes.
   *
   * The same will be true in the "sparse" scenario where you have a small set of
   * random-looking integers spanning a wide range of values.
   *
   * These are considered adversarial cases because, as a general rule,
   * if your data looks like a set
   * of random integers, Roaring bitmaps are probably not the right data structure.
   *
   * Note that you can serialize your Roaring Bitmaps to disk and then construct
   * ImmutableRoaringBitmap instances from a ByteBuffer. In such cases, the Java heap
   * usage will be significantly less than
   * what is reported.
   *
   * If your main goal is to compress arrays of integers, there are other libraries
   * that are maybe more appropriate
   * such as JavaFastPFOR.
   *
   * Note, however, that in general, random integers (as produced by random number
   * generators or hash functions) are not compressible.
   * Trying to compress random data is an adversarial use case.
   *
   * @see <a href="https://github.com/lemire/JavaFastPFOR">JavaFastPFOR</a>
   *
   *
   * @return estimated memory usage.
   */
  @Override
  public long getLongSizeInBytes() {
    long size = 8;
    for (int i = 0; i < this.size(); i++) {
      final Container c = this.getContainerAtIndex(i);
      size += 2 + c.getSizeInBytes();
    }
    return size;
  }

  /**
   * Estimate of the memory usage of this data structure. This can be expected to be within 1% of
   * the true memory usage in common usage scenarios.
   * If exact measures are needed, we recommend using dedicated libraries
   * such as ehcache-sizeofengine.
   *
   * In adversarial cases, this estimate may be 10x the actual memory usage. For example, if
   * you insert a single random value in a bitmap, then over a 100 bytes may be used by the JVM
   * whereas this function may return an estimate of 32 bytes.
   *
   * The same will be true in the "sparse" scenario where you have a small set of
   * random-looking integers spanning a wide range of values.
   *
   * These are considered adversarial cases because, as a general rule,
   * if your data looks like a set
   * of random integers, Roaring bitmaps are probably not the right data structure.
   *
   * Note that you can serialize your Roaring Bitmaps to disk and then construct
   * ImmutableRoaringBitmap instances from a ByteBuffer. In such cases, the Java heap
   * usage will be significantly less than
   * what is reported.
   *
   * If your main goal is to compress arrays of integers, there are other libraries
   * that are maybe more appropriate
   * such as JavaFastPFOR.
   *
   * Note, however, that in general, random integers (as produced by random number
   * generators or hash functions) are not compressible.
   * Trying to compress random data is an adversarial use case.
   *
   * @see <a href="https://github.com/lemire/JavaFastPFOR">JavaFastPFOR</a>
   *
   *
   * @return estimated memory usage.
   */
  @Override
  public int getSizeInBytes() {
    return (int) getLongSizeInBytes() ;
  }


  /**
   * Check whether this bitmap has had its runs compressed.
   *
   * @return whether this bitmap has run compression
   */
  public boolean hasRunCompression() {
    for (int i = 0; i < this.size(); i++) {
      Container c = this.getContainerAtIndex(i);
      if (c instanceof RunContainer) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether the bitmap is empty.
   *
   * @return true if this bitmap contains no set bit
   */
  @Override
  public boolean isEmpty() {
    return size() == 0;
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

      private CharIterator iter;

      private int pos = 0;

      private int x;

      @Override
      public boolean hasNext() {
        return pos < RoaringBitmap.this.size();
      }

      private Iterator<Integer> init() {
        if (pos < RoaringBitmap.this.size()) {
          iter = RoaringBitmap.this.getContainerAtIndex(pos).getCharIterator();
          hs = RoaringBitmap.this.getKeyAtIndex(pos) << 16;
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
        // todo: implement
        throw new UnsupportedOperationException();
      }

    }.init();
  }


  // don't forget to call repairAfterLazy() afterward
  // important: x2 should not have been computed lazily
  protected void lazyor(final RoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = size();
    final int length2 = x2.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = getKeyAtIndex(pos1);
      char s2 = x2.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          this.setContainerAtIndex(pos1, getContainerAtIndex(pos1)
              .lazyIOR(x2.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = getKeyAtIndex(pos1);
          s2 = x2.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = getKeyAtIndex(pos1);
        } else { 
          insertNewKeyValueAt(pos1, s2,
              x2.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      appendCopy(x2, pos2, length2);
    }
  }

  // don't forget to call repairAfterLazy() afterward
  // important: x2 should not have been computed lazily
  // this method is like lazyor except that it will convert
  // the current container to a bitset
  protected void naivelazyor(RoaringBitmap  x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = size();
    final int length2 = x2.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = getKeyAtIndex(pos1);
      char s2 = x2.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          BitmapContainer c1 = getContainerAtIndex(pos1).toBitmapContainer();
          this.setContainerAtIndex(pos1,
              c1.lazyIOR(x2.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = getKeyAtIndex(pos1);
          s2 = x2.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = getKeyAtIndex(pos1);
        } else { 
          insertNewKeyValueAt(pos1, s2,
              x2.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      appendCopy(x2, pos2, length2);
    }
  }

  /**
   * Create a new Roaring bitmap containing at most maxcardinality integers.
   *
   * @param maxcardinality maximal cardinality
   * @return a new bitmap with cardinality no more than maxcardinality
   */
  @Override
  public RoaringBitmap limit(int maxcardinality) {
    RoaringBitmap answer = new RoaringBitmap();
    int currentcardinality = 0;
    for (int i = 0; (currentcardinality < maxcardinality)
        && (i < this.size()); i++) {
      Container c = this.getContainerAtIndex(i);
      if (c.getCardinality() + currentcardinality <= maxcardinality) {
        answer.appendCopy(this, i);
        currentcardinality += c.getCardinality();
      } else {
        int leftover = maxcardinality - currentcardinality;
        Container limited = c.limit(leftover);
        answer.append(this.getKeyAtIndex(i), limited);
        break;
      }
    }
    return answer;
  }

  /**
   * In-place bitwise OR (union) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void or(final RoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = size();
    final int length2 = x2.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = getKeyAtIndex(pos1);
      char s2 = x2.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          this.setContainerAtIndex(pos1, getContainerAtIndex(pos1)
              .ior(x2.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = getKeyAtIndex(pos1);
          s2 = x2.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = getKeyAtIndex(pos1);
        } else { 
          insertNewKeyValueAt(pos1, s2,
              x2.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      appendCopy(x2, pos2, length2);
    }
  }



  /**
   * Computes OR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   */
  public static RoaringBitmap or(final Iterator<? extends RoaringBitmap> bitmaps,
      final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);

    Iterator<RoaringBitmap> bitmapsIterator;
    bitmapsIterator = selectRangeWithoutCopy(bitmaps, rangeStart, rangeEnd);
    return or(bitmapsIterator);
  }


  /**
   * Computes OR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   * @deprecated use the version where longs specify the range.
   *     Negative range points are forbidden.
   */
  @Deprecated
  public static RoaringBitmap or(final Iterator<? extends RoaringBitmap> bitmaps,
          final int rangeStart, final int rangeEnd) {
    return or(bitmaps, (long) rangeStart, (long) rangeEnd);
  }


  /**
   * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be
   * GetCardinality()).
   *
   * @param x upper limit
   *
   * @return the rank
   * @see <a href="https://en.wikipedia.org/wiki/Ranking#Ranking_in_statistics">Ranking in statistics</a>
   */
  @Override
  public long rankLong(int x) {
    long size = 0;
    char xhigh = Util.highbits(x);

    for (int i = 0; i < this.size(); i++) {
      char key = this.getKeyAtIndex(i);
      if (key < xhigh) {
        size += this.getContainerAtIndex(i).getCardinality();
      } else if (key == xhigh) {
        return size + this.getContainerAtIndex(i).rank(Util.lowbits(x));
      }
    }
    return size;
  }

  @Override
  public long rangeCardinality(long start, long end) {
    if(Long.compareUnsigned(start, end) >= 0) {
      return 0;
    }
    long size = 0;
    int startIndex = this.getIndex(Util.highbits(start));
    if(startIndex < 0)  {
      startIndex = -startIndex - 1;
    } else {
      int inContainerStart = (Util.lowbits(start));
      if(inContainerStart != 0) {
        size -= this
          .getContainerAtIndex(startIndex)
          .rank((char)(inContainerStart - 1));
      }
    }
    char xhigh = Util.highbits(end - 1);
    for (int i = startIndex; i < this.size(); i++) {
      char key = this.getKeyAtIndex(i);
      if (key < xhigh) {
        size += this.getContainerAtIndex(i).getCardinality();
      } else if (key == xhigh) {
        return size + this
          .getContainerAtIndex(i).rank(Util.lowbits((int)(end - 1)));
      }
    }
    return size;
  }



  @Override
  public int rank(int x) {
    return (int) rankLong(x);
  }

  /**
   * If present remove the specified integer (effectively, sets its bit value to false)
   *
   * @param x integer value representing the index in a bitmap
   */
  @Override
  public void remove(final int x) {
    final char hb = Util.highbits(x);
    final int i = getIndex(hb);
    if (i < 0) {
      return;
    }
    setContainerAtIndex(i,
        getContainerAtIndex(i).remove(Util.lowbits(x)));
    if (getContainerAtIndex(i).isEmpty()) {
      removeAtIndex(i);
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


    final int hbStart = (Util.highbits(rangeStart));
    final int lbStart = (Util.lowbits(rangeStart));
    final int hbLast = (Util.highbits(rangeEnd - 1));
    final int lbLast = (Util.lowbits(rangeEnd - 1));
    if (hbStart == hbLast) {
      final int i = getIndex((char) hbStart);
      if (i < 0) {
        return;
      }
      final Container c = getContainerAtIndex(i).iremove(lbStart, lbLast + 1);
      if (!c.isEmpty()) {
        setContainerAtIndex(i, c);
      } else {
        removeAtIndex(i);
      }
      return;
    }
    int ifirst = getIndex((char) hbStart);
    int ilast = getIndex((char) hbLast);
    if (ifirst >= 0) {
      if (lbStart != 0) {
        final Container c = getContainerAtIndex(ifirst).iremove(lbStart,
            Util.maxLowBitAsInteger() + 1);
        if (!c.isEmpty()) {
          setContainerAtIndex(ifirst, c);
          ifirst++;
        }
      }
    } else {
      ifirst = -ifirst - 1;
    }
    if (ilast >= 0) {
      if (lbLast != Util.maxLowBitAsInteger()) {
        final Container c = getContainerAtIndex(ilast).iremove(0, lbLast + 1);
        if (!c.isEmpty()) {
          setContainerAtIndex(ilast, c);
        } else {
          ilast++;
        }
      } else {
        ilast++;
      }
    } else {
      ilast = -ilast - 1;
    }
    removeIndexRange(ifirst, ilast);
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
    for (int i = 0; i < this.size(); i++) {
      Container c = this.getContainerAtIndex(i);
      if (c instanceof RunContainer) {
        Container newc = ((RunContainer) c).toBitmapOrArrayContainer(c.getCardinality());
        this.setContainerAtIndex(i, newc);
        answer = true;
      }
    }
    return answer;
  }

  // to be used with lazyor
  protected void repairAfterLazy() {
    for (int k = 0; k < size(); ++k) {
      Container c = getContainerAtIndex(k);
      setContainerAtIndex(k, c.repairAfterLazy());
    }
  }

  /**
   * Use a run-length encoding where it is more space efficient
   *
   * @return whether a change was applied
   */
  public boolean runOptimize() {
    boolean answer = false;
    for (int i = 0; i < this.size(); i++) {
      Container c = this.getContainerAtIndex(i).runOptimize();
      if (c instanceof RunContainer) {
        answer = true;
      }
      this.setContainerAtIndex(i, c);
    }
    return answer;
  }

  /**
   * Checks whether the parameter is a subset of this RoaringBitmap or not
   * @param subset the potential subset
   * @return true if the parameter is a subset of this RoaringBitmap
   */
  public boolean contains(RoaringBitmap subset) {
    if(subset.getCardinality() > getCardinality()) {
      return false;
    }
    final int length1 = this.size;
    final int length2 = subset.size;
    int pos1 = 0, pos2 = 0;
    while (pos1 < length1 && pos2 < length2) {
      final char s1 = this.getKeyAtIndex(pos1);
      final char s2 = subset.getKeyAtIndex(pos2);
      if (s1 == s2) {
        Container c1 = this.getContainerAtIndex(pos1);
        Container c2 = subset.getContainerAtIndex(pos2);
        if(!c1.contains(c2)) {
          return false;
        }
        ++pos1;
        ++pos2;
      } else if (s1 - s2 > 0) {
        return false;
      } else {
        pos1 = subset.advanceUntil(s2, pos1);
      }
    }
    return pos2 == length2;
  }


  /**
   * Return the jth value stored in this bitmap. The provided value
   * needs to be smaller than the cardinality otherwise an
   * IllegalArgumentException
   * exception is thrown.
   *
   * @param j index of the value
   *
   * @return the value
   * @see <a href="https://en.wikipedia.org/wiki/Selection_algorithm">Selection algorithm</a>
   */
  @Override
  public int select(int j) {
    long leftover = Util.toUnsignedLong(j);
    for (int i = 0; i < this.size(); i++) {
      Container c = this.getContainerAtIndex(i);
      int thiscard = c.getCardinality();
      if (thiscard > leftover) {
        int keycontrib = this.getKeyAtIndex(i) << 16;
        int lowcontrib = (c.select((int)leftover));
        return lowcontrib + keycontrib;
      }
      leftover -= thiscard;
    }
    throw new IllegalArgumentException("You are trying to select the "
                 + j + "th value when the cardinality is "
                 + this.getCardinality() + ".");
  }

  @Override
  public long nextValue(int fromValue) {
    char key = Util.highbits(fromValue);
    int containerIndex = advanceUntil(key, -1);
    long nextSetBit = -1L;
    while (containerIndex < size() && nextSetBit == -1L) {
      char containerKey = getKeyAtIndex(containerIndex);
      Container container = getContainerAtIndex(containerIndex);
      int bit = (containerKey - key > 0
              ? container.first()
              : container.nextValue(Util.lowbits(fromValue)));
      nextSetBit = bit == -1 ? -1L : Util.toUnsignedLong((containerKey << 16) | bit);
      ++containerIndex;
    }
    assert nextSetBit <= 0xFFFFFFFFL;
    assert nextSetBit == -1L || nextSetBit >= Util.toUnsignedLong(fromValue);
    return nextSetBit;
  }

  @Override
  public long previousValue(int fromValue) {
    char key = Util.highbits(fromValue);
    int containerIndex = advanceUntil(key, -1);
    if (containerIndex == size()) {
      return -1L;
    }
    long prevSetBit = -1L;
    while (containerIndex != -1 && prevSetBit == -1L) {
      char containerKey = getKeyAtIndex(containerIndex);
      Container container = getContainerAtIndex(containerIndex);
      int bit = (containerKey < key
              ? container.last()
              : container.previousValue(Util.lowbits(fromValue)));
      prevSetBit = bit == -1 ? -1L : Util.toUnsignedLong((containerKey << 16) | bit);
      --containerIndex;
    }
    assert prevSetBit <= 0xFFFFFFFFL;
    assert prevSetBit <= Util.toUnsignedLong(fromValue);
    return prevSetBit;
  }

  @Override
  public long nextAbsentValue(int fromValue) {
    long nextAbsentBit = computeNextAbsentValue(fromValue);
    assert nextAbsentBit <= 0xFFFFFFFFL;
    assert nextAbsentBit >= Util.toUnsignedLong(fromValue);
    assert !contains((int) nextAbsentBit);
    return nextAbsentBit;
  }

  private long computeNextAbsentValue(int fromValue) {
    char key = Util.highbits(fromValue);
    int containerIndex = advanceUntil(key, -1);

    int size = size();
    if (containerIndex == size) {
      return Util.toUnsignedLong(fromValue);
    }
    char containerKey = getKeyAtIndex(containerIndex);
    if (fromValue < containerKey << 16) {
      return Util.toUnsignedLong(fromValue);
    }
    Container container = getContainerAtIndex(containerIndex);
    int bit = container.nextAbsentValue(Util.lowbits(fromValue));
    while (true) {
      if (bit != 1 << 16) {
        return Util.toUnsignedLong((containerKey << 16) | bit);
      }
      assert container.last() == (1 << 16) - 1;
      if (containerIndex == size - 1) {
        return Util.toUnsignedLong(last()) + 1;
      }

      containerIndex += 1;
      char nextContainerKey = getKeyAtIndex(containerIndex);
      if (containerKey + 1 < nextContainerKey) {
        return Util.toUnsignedLong((containerKey + 1) << 16);
      }
      containerKey = nextContainerKey;
      container = getContainerAtIndex(containerIndex);
      bit = container.nextAbsentValue((char) 0);
    }
  }

  @Override
  public long previousAbsentValue(int fromValue) {
    long prevAbsentBit = computePreviousAbsentValue(fromValue);
    assert prevAbsentBit <= 0xFFFFFFFFL;
    assert prevAbsentBit <= Util.toUnsignedLong(fromValue);
    assert !contains((int) prevAbsentBit);
    return prevAbsentBit;
  }

  private long computePreviousAbsentValue(int fromValue) {
    char key = Util.highbits(fromValue);
    int containerIndex = advanceUntil(key, -1);

    if (containerIndex == size()) {
      return Util.toUnsignedLong(fromValue);
    }
    char containerKey = getKeyAtIndex(containerIndex);
    if (fromValue < containerKey << 16) {
      return Util.toUnsignedLong(fromValue);
    }
    Container container = getContainerAtIndex(containerIndex);
    int bit = container.previousAbsentValue(Util.lowbits(fromValue));

    while (true) {
      if (bit != -1) {
        return Util.toUnsignedLong((containerKey << 16) | bit);
      }
      assert container.first() == 0;
      if (containerIndex == 0) {
        return Util.toUnsignedLong(first()) - 1;
      }

      containerIndex -= 1;
      char nextContainerKey = getKeyAtIndex(containerIndex);
      if (nextContainerKey < containerKey - 1) {
        return Util.toUnsignedLong((containerKey << 16)) - 1;
      }
      containerKey = nextContainerKey;
      container = getContainerAtIndex(containerIndex);
      bit = container.previousAbsentValue((char) ((1 << 16) - 1));
    }
  }

  /**
   * Serialize this bitmap.
   *
   *  See format specification at https://github.com/RoaringBitmap/RoaringFormatSpec
   *
   * Consider calling {@link #runOptimize} before serialization to improve compression.
   *
   * The current bitmap is not modified.
   *
   * There is a distinct and dedicated method to serialize to a ByteBuffer.
   *
   * Note: Java's data structures are in big endian format. Roaring serializes to a little endian
   * format, so the bytes are flipped by the library during serialization to ensure that what is
   * stored is in little endian---despite Java's big endianness. You can defeat this process by
   * reflipping the bytes again in a custom DataOutput which could lead to serialized Roaring
   * objects with an incorrect byte order.
   *
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Override
  public void serialize(DataOutput out) throws IOException {
    this.serializeContent(out);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    serializeContent(buffer);
  }


  /**
   * Assume that one wants to store "cardinality" integers in [0, universe_size), this function
   * returns an upper bound on the serialized size in bytes.
   *
   * @param cardinality maximal cardinality
   * @param universe_size maximal value
   * @return upper bound on the serialized size in bytes of the bitmap
   */
  public static long maximumSerializedSize(long cardinality, long universe_size) {
    long contnbr = (universe_size + 65535) / 65536;
    if (contnbr > cardinality) {
      contnbr = cardinality;
      // we can't have more containers than we have values
    }
    final long headermax = Math.max(8, 4 + (contnbr + 7) / 8) + 8 * contnbr;
    final long valsarray = 2 * cardinality;
    final long valsbitmap = contnbr * 8192;
    final long valsbest = Math.min(valsarray, valsbitmap);
    return valsbest + headermax;
  }

  /**
   * Return new iterator with only values from rangeStart (inclusive) to rangeEnd (exclusive)
   *
   * @param bitmaps bitmaps iterator
   * @param rangeStart inclusive
   * @param rangeEnd exclusive
   * @return new iterator of bitmaps
   */
  private static Iterator<RoaringBitmap> selectRangeWithoutCopy(final
      Iterator<? extends RoaringBitmap> bitmaps,
      final long rangeStart, final long rangeEnd) {
    Iterator<RoaringBitmap> bitmapsIterator;
    bitmapsIterator = new Iterator<RoaringBitmap>() {
      @Override
      public boolean hasNext() {
        return bitmaps.hasNext();
      }

      @Override
      public RoaringBitmap next() {
        RoaringBitmap next = bitmaps.next();
        return selectRangeWithoutCopy(next, rangeStart, rangeEnd);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Remove not supported");
      }
    };
    return bitmapsIterator;
  }


  /**
   *
   * Extracts the values in the specified range, rangeStart (inclusive) and rangeEnd (exclusive)
   * while avoiding copies as much as possible.
   *
   * @param rb input bitmap
   * @param rangeStart inclusive
   * @param rangeEnd exclusive
   * @return new bitmap
   */

  // had formerly failed if rangeEnd==0
  private static RoaringBitmap selectRangeWithoutCopy(RoaringBitmap rb, final long rangeStart,
      final long rangeEnd) {
    final int hbStart = (Util.highbits(rangeStart));
    final int lbStart = (Util.lowbits(rangeStart));
    final int hbLast = (Util.highbits(rangeEnd - 1));
    final int lbLast = (Util.lowbits(rangeEnd - 1));
    RoaringBitmap answer = new RoaringBitmap();

    assert(rangeStart >= 0 && rangeEnd >= 0);

    if (rangeEnd <= rangeStart) {
      return answer;
    }

    if (hbStart == hbLast) {
      final int i = rb.getIndex((char) hbStart);
      if (i >= 0) {
        final Container c = rb.getContainerAtIndex(i).remove(0, lbStart)
            .iremove(lbLast + 1, Util.maxLowBitAsInteger() + 1);
        if (!c.isEmpty()) {
          answer.append((char) hbStart, c);
        }
      }
      return answer;
    }
    int ifirst = rb.getIndex((char) hbStart);
    int ilast = rb.getIndex((char) hbLast);
    if (ifirst >= 0) {
      final Container c = rb.getContainerAtIndex(ifirst).remove(0, lbStart);
      if (!c.isEmpty()) {
        answer.append((char) hbStart, c);
      }
    }

    // revised to loop on ints
    for (int hb = hbStart + 1; hb <= hbLast - 1; ++hb) {
      final int i = rb.getIndex((char)hb);
      final int j = answer.getIndex((char) hb);
      assert j < 0;

      if (i >= 0) {
        final Container c = rb.getContainerAtIndex(i);
        answer.insertNewKeyValueAt(-j - 1, (char)hb, c);
      }
    }

    if (ilast >= 0) {
      final Container c = rb.getContainerAtIndex(ilast).remove(lbLast + 1,
          Util.maxLowBitAsInteger() + 1);
      if (!c.isEmpty()) {
        answer.append((char) hbLast, c);
      }
    }
    return answer;
  }


  /**
   * Return the set values as an array, if the cardinality is smaller than 2147483648.
   * The integer values are in sorted order.
   *
   * @return array representing the set values.
   */
  @Override
  public int[] toArray() {
    final int[] array = new int[this.getCardinality()];
    int pos = 0, pos2 = 0;
    while (pos < this.size()) {
      final int hs = this.getKeyAtIndex(pos) << 16;
      Container c = this.getContainerAtIndex(pos++);
      c.fillLeastSignificant16bits(array, pos2, hs);
      pos2 += c.getCardinality();
    }
    return array;
  }

  /**
   *
   * Convert (copies) to a mutable roaring bitmap.
   *
   * @return a copy of this bitmap as a MutableRoaringBitmap
   */
  public MutableRoaringBitmap toMutableRoaringBitmap() {
    return new MutableRoaringBitmap(this);
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
    if (i.hasNext()) {
      answer.append(i.next() & 0xFFFFFFFFL);
    }
    while (i.hasNext()) {
      answer.append(",");
      // to avoid using too much memory, we limit the size
      if(answer.length() > 0x80000) {
        answer.append("...");
        break;
      }
      answer.append(i.next() & 0xFFFFFFFFL);

    }
    answer.append("}");
    return answer.toString();
  }

  /**
   * In-place bitwise XOR (symmetric difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void xor(final RoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = size();
    final int length2 = x2.size();

    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = getKeyAtIndex(pos1);
      char s2 = x2.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          final Container c = getContainerAtIndex(pos1)
              .ixor(x2.getContainerAtIndex(pos2));
          if (!c.isEmpty()) {
            this.setContainerAtIndex(pos1, c);
            pos1++;
          } else {
            removeAtIndex(pos1);
            --length1;
          }
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = getKeyAtIndex(pos1);
          s2 = x2.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = getKeyAtIndex(pos1);
        } else { 
          insertNewKeyValueAt(pos1, s2,
              x2.getContainerAtIndex(pos2).clone());
          pos1++;
          length1++;
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      appendCopy(x2, pos2, length2);
    }
  }

  /**
   * Computes XOR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   */
  public static RoaringBitmap xor(final Iterator<? extends RoaringBitmap> bitmaps,
      final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    Iterator<RoaringBitmap> bitmapsIterator;
    bitmapsIterator = selectRangeWithoutCopy(bitmaps, rangeStart, rangeEnd);
    return FastAggregation.xor(bitmapsIterator);
  }

  /**
   * Computes XOR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bi
   * @deprecated use the version where longs specify the range.
   *     Negative values not allowed for rangeStart and rangeEnd
   */
  @Deprecated
  public static RoaringBitmap xor(final Iterator<? extends RoaringBitmap> bitmaps,
          final int rangeStart, final int rangeEnd) {
    return xor(bitmaps, (long) rangeStart, (long) rangeEnd);
  }


}
