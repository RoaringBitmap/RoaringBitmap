/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MappeableContainerPointer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator.OfInt;
import java.util.Spliterator;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import java.util.Spliterators;

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


public class RoaringBitmap implements Cloneable, Serializable, Iterable<Integer>, Externalizable,
    ImmutableBitmapDataProvider, BitmapDataProvider, AppendableStorage<Container> {

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
      return pos < RoaringBitmap.this.highLowContainer.size();
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
      if (pos < RoaringBitmap.this.highLowContainer.size()) {
        iter = RoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).getCharIterator();
        hs = RoaringBitmap.this.highLowContainer.getKeyAtIndex(pos) << 16;
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

    int pos = RoaringBitmap.this.highLowContainer.size() - 1;

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
            RoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).getReverseCharIterator();
        hs = RoaringBitmap.this.highLowContainer.getKeyAtIndex(pos) << 16;
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
   * Generate a copy of the provided bitmap, but with
   * all its values incremented by offset.
   * The parameter offset can be
   * negative. Values that would fall outside
   * of the valid 32-bit range are discarded
   * so that the result can have lower cardinality.
   * 
   * This method can be relatively expensive when
   * offset is not divisible by 65536. Use sparingly.
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
    if((container_offset_long < -(1<<16) ) || (container_offset_long >= (1<<16) )) {
      return new RoaringBitmap(); // it is necessarily going to be empty
    }
    // next cast is necessarily safe, the result is between -0xFFFF and 0xFFFF
    int container_offset = (int) container_offset_long;
    // next case is safe
    int in_container_offset = (int)(offset - container_offset_long * (1L<<16));
    if(in_container_offset == 0) {
      RoaringBitmap answer = new RoaringBitmap();
      for(int pos = 0; pos < x.highLowContainer.size(); pos++) {
        int key = (x.highLowContainer.getKeyAtIndex(pos));
        key += container_offset;
        answer.highLowContainer.append((char)key,
            x.highLowContainer.getContainerAtIndex(pos).clone());
      }
      return answer;
    } else {
      RoaringBitmap answer = new RoaringBitmap();
      for(int pos = 0; pos < x.highLowContainer.size(); pos++) {
        int key = (x.highLowContainer.getKeyAtIndex(pos));
        key += container_offset;
        Container c = x.highLowContainer.getContainerAtIndex(pos);
        Container[] offsetted = Util.addOffset(c,
                (char)in_container_offset);
        boolean keyok = (key >= 0) && (key <= 0xFFFF);
        boolean keypok = (key + 1 >= 0) && (key + 1 <= 0xFFFF);
        if( !offsetted[0].isEmpty() && keyok) {
          int current_size = answer.highLowContainer.size();
          int lastkey = 0;
          if(current_size > 0) {
            lastkey = (answer.highLowContainer.getKeyAtIndex(
                    current_size - 1));
          }
          if((current_size > 0) && (lastkey == key)) {
            Container prev = answer.highLowContainer
                    .getContainerAtIndex(current_size - 1);
            Container orresult = prev.ior(offsetted[0]);
            answer.highLowContainer.setContainerAtIndex(current_size - 1,
                    orresult);
          } else {
            answer.highLowContainer.append((char)key, offsetted[0]);
          }
        }
        if( !offsetted[1].isEmpty()  && keypok) {
          answer.highLowContainer.append((char)(key + 1), offsetted[1]);
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
    answer.highLowContainer.appendCopiesUntil(rb.highLowContainer, (char) hbStart);

    if (hbStart == hbLast) {
      final int i = rb.highLowContainer.getIndex((char) hbStart);
      final Container c =
          i >= 0 ? rb.highLowContainer.getContainerAtIndex(i).add(lbStart, lbLast + 1)
              : Container.rangeOfOnes(lbStart, lbLast + 1);
      answer.highLowContainer.append((char) hbStart, c);
      answer.highLowContainer.appendCopiesAfter(rb.highLowContainer, (char) hbLast);
      return answer;
    }
    int ifirst = rb.highLowContainer.getIndex((char) hbStart);
    int ilast = rb.highLowContainer.getIndex((char) hbLast);

    {
      final Container c = ifirst >= 0
          ? rb.highLowContainer.getContainerAtIndex(ifirst).add(lbStart,
              Util.maxLowBitAsInteger() + 1)
          : Container.rangeOfOnes(lbStart, Util.maxLowBitAsInteger() + 1);
      answer.highLowContainer.append((char) hbStart, c);
    }
    for (int hb = hbStart + 1; hb < hbLast; ++hb) {
      Container c = Container.rangeOfOnes(0, Util.maxLowBitAsInteger() + 1);
      answer.highLowContainer.append((char) hb, c);
    }
    {
      final Container c =
          ilast >= 0 ? rb.highLowContainer.getContainerAtIndex(ilast).add(0, lbLast + 1)
              : Container.rangeOfOnes(0, lbLast + 1);
      answer.highLowContainer.append((char) hbLast, c);
    }
    answer.highLowContainer.appendCopiesAfter(rb.highLowContainer, (char) hbLast);
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
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        final Container c = c1.and(c2);
        if (!c.isEmpty()) {
          answer.highLowContainer.append(s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
      } else { 
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
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
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        // TODO: could be made faster if we did not have to materialize container
        answer += c1.andCardinality(c2);
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
      } else { 
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
  public static RoaringBitmap andNot(final RoaringBitmap x1, final RoaringBitmap x2) {
    final RoaringBitmap answer = new RoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        final Container c = c1.andNot(c2);
        if (!c.isEmpty()) {
          answer.highLowContainer.append(s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        final int nextPos1 = x1.highLowContainer.advanceUntil(s2, pos1);
        answer.highLowContainer.appendCopy(x1.highLowContainer, pos1, nextPos1);
        pos1 = nextPos1;
      } else { 
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    if (pos2 == length2) {
      answer.highLowContainer.appendCopy(x1.highLowContainer, pos1, length1);
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
   * faster than calling "add" repeatedly on the values dat[offset], dat[offset+1],..., 
   * dat[offset+n-1]. 
   * The provided integers values don't have to be in sorted order, but it may be preferable 
   * to sort them from a performance point of view. 
   *
   * @param dat set values
   * @param offset from which index the values should be set to true
   * @param n how many values should be set to true
   */
  public void addN(final int[] dat, final int offset, final int n) {
    // let us validate the values first.
    if((n < 0) || (offset < 0)) {
      throw new IllegalArgumentException("Negative values do not make sense.");
    }
    if(n == 0) {
      return; // nothing to do
    }
    if(offset + n > dat.length) {
      throw new IllegalArgumentException("Data source is too small.");
    }
    Container currentcont = null;
    int j = 0;
    int val = dat[j + offset];
    char currenthb = Util.highbits(val);
    int currentcontainerindex = highLowContainer.getIndex(currenthb);
    if (currentcontainerindex >= 0) {
      currentcont = highLowContainer.getContainerAtIndex(currentcontainerindex);
      Container newcont = currentcont.add(Util.lowbits(val));
      if(newcont != currentcont) {
        highLowContainer.setContainerAtIndex(currentcontainerindex, newcont);
        currentcont = newcont;
      }
    } else {
      currentcontainerindex = - currentcontainerindex - 1;
      final ArrayContainer newac = new ArrayContainer();
      currentcont = newac.add(Util.lowbits(val));
      highLowContainer.insertNewKeyValueAt(currentcontainerindex, currenthb, currentcont);
    }
    j++;
    for( ; j < n; ++j) {
      val = dat[j + offset];
      char newhb = Util.highbits(val);
      if(currenthb == newhb) {// easy case
        // this could be quite frequent
        Container newcont = currentcont.add(Util.lowbits(val));
        if(newcont != currentcont) {
          highLowContainer.setContainerAtIndex(currentcontainerindex, newcont);
          currentcont = newcont;
        }
      } else {
        currenthb = newhb;
        currentcontainerindex = highLowContainer.getIndex(currenthb);
        if (currentcontainerindex >= 0) {
          currentcont = highLowContainer.getContainerAtIndex(currentcontainerindex);
          Container newcont = currentcont.add(Util.lowbits(val));
          if(newcont != currentcont) {
            highLowContainer.setContainerAtIndex(currentcontainerindex, newcont);
            currentcont = newcont;
          }
        } else {
          currentcontainerindex = - currentcontainerindex - 1;
          final ArrayContainer newac = new ArrayContainer();
          currentcont = newac.add(Util.lowbits(val));
          highLowContainer.insertNewKeyValueAt(currentcontainerindex, currenthb, currentcont);
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
    answer.highLowContainer.appendCopiesUntil(bm.highLowContainer, (char) hbStart);

    for (int hb = hbStart; hb <= hbLast; ++hb) {
      final int containerStart = (hb == hbStart) ? lbStart : 0;
      final int containerLast = (hb == hbLast) ? lbLast : Util.maxLowBitAsInteger();

      final int i = bm.highLowContainer.getIndex((char) hb);
      final int j = answer.highLowContainer.getIndex((char) hb);
      assert j < 0;

      if (i >= 0) {
        Container c =
            bm.highLowContainer.getContainerAtIndex(i).not(containerStart, containerLast + 1);
        if (!c.isEmpty()) {
          answer.highLowContainer.insertNewKeyValueAt(-j - 1, (char) hb, c);
        }

      } else { // *think* the range of ones must never be
        // empty.
        answer.highLowContainer.insertNewKeyValueAt(-j - 1, (char) hb,
            Container.rangeOfOnes(containerStart, containerLast + 1));
      }
    }
    // copy the containers after the active area.
    answer.highLowContainer.appendCopiesAfter(bm.highLowContainer, (char) hbLast);
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
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        if (c1.intersects(c2)) {
          return true;
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
      } else { 
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    return false;
  }


  // important: inputs should not have been computed lazily
  protected static RoaringBitmap lazyor(final RoaringBitmap x1, final RoaringBitmap x2) {
    final RoaringBitmap answer = new RoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          answer.highLowContainer.append(s1, x1.highLowContainer.getContainerAtIndex(pos1)
              .lazyOR(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          answer.highLowContainer.appendCopy(x1.highLowContainer, pos1);
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
        } else { 
          answer.highLowContainer.appendCopy(x2.highLowContainer, pos2);
          pos2++;
          if (pos2 == length2) {
            break main;
          }
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

  // important: inputs should not be reused
  protected static RoaringBitmap lazyorfromlazyinputs(final RoaringBitmap x1,
      final RoaringBitmap x2) {
    final RoaringBitmap answer = new RoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
          Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
          if ((c2 instanceof BitmapContainer) && (!(c1 instanceof BitmapContainer))) {
            Container tmp = c1;
            c1 = c2;
            c2 = tmp;
          }
          answer.highLowContainer.append(s1, c1.lazyIOR(c2));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
          answer.highLowContainer.append(s1, c1);
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
        } else { 
          Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
          answer.highLowContainer.append(s2,c2);
          pos2++;
          if (pos2 == length2) {
            break main;
          }
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        }
      }
    }
    if (pos1 == length1) {
      answer.highLowContainer.append(x2.highLowContainer, pos2, length2);
    } else if (pos2 == length2) {
      answer.highLowContainer.append(x1.highLowContainer, pos1, length1);
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
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          answer.highLowContainer.append(s1, x1.highLowContainer.getContainerAtIndex(pos1)
              .or(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          answer.highLowContainer.appendCopy(x1.highLowContainer, pos1);
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
        } else { 
          answer.highLowContainer.appendCopy(x2.highLowContainer, pos2);
          pos2++;
          if (pos2 == length2) {
            break main;
          }
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
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    if (length2 > 4 * length1) {
      // if x1 is much smaller than x2, this can be much faster
      return x1.getCardinality() - andCardinality(x1, x2);
    }

    long cardinality = 0L;
    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
      char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      char s2 = x2.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        cardinality += c1.getCardinality() - c1.andCardinality(c2);
        ++pos1;
        ++pos2;
      } else if (s1 < s2) {
        while (s1 < s2 && pos1 < length1) {
          cardinality += x1.highLowContainer.getContainerAtIndex(pos1).getCardinality();
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
          ++pos1;
        }
      } else {
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    if (pos2 == length2) {
      while (pos1 < length1) {
        cardinality += x1.highLowContainer.getContainerAtIndex(pos1).getCardinality();
        ++pos1;
      }
    }
    return (int)cardinality;
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
    answer.highLowContainer.appendCopiesUntil(rb.highLowContainer, (char) hbStart);

    if (hbStart == hbLast) {
      final int i = rb.highLowContainer.getIndex((char) hbStart);
      if (i >= 0) {
        final Container c = rb.highLowContainer.getContainerAtIndex(i).remove(lbStart, lbLast + 1);
        if (!c.isEmpty()) {
          answer.highLowContainer.append((char) hbStart, c);
        }
      }
      answer.highLowContainer.appendCopiesAfter(rb.highLowContainer, (char) hbLast);
      return answer;
    }
    int ifirst = rb.highLowContainer.getIndex((char) hbStart);
    int ilast = rb.highLowContainer.getIndex((char) hbLast);
    if ((ifirst >= 0) && (lbStart != 0)) {
      final Container c = rb.highLowContainer.getContainerAtIndex(ifirst).remove(lbStart,
          Util.maxLowBitAsInteger() + 1);
      if (!c.isEmpty()) {
        answer.highLowContainer.append((char) hbStart, c);
      }
    }
    if ((ilast >= 0) && (lbLast != Util.maxLowBitAsInteger())) {
      final Container c = rb.highLowContainer.getContainerAtIndex(ilast).remove(0, lbLast + 1);
      if (!c.isEmpty()) {
        answer.highLowContainer.append((char) hbLast, c);
      }
    }
    answer.highLowContainer.appendCopiesAfter(rb.highLowContainer, (char) hbLast);
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
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          final Container c = x1.highLowContainer.getContainerAtIndex(pos1)
              .xor(x2.highLowContainer.getContainerAtIndex(pos2));
          if (!c.isEmpty()) {
            answer.highLowContainer.append(s1, c);
          }
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          answer.highLowContainer.appendCopy(x1.highLowContainer, pos1);
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = x1.highLowContainer.getKeyAtIndex(pos1);
        } else { 
          answer.highLowContainer.appendCopy(x2.highLowContainer, pos2);
          pos2++;
          if (pos2 == length2) {
            break main;
          }
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
   * Wrap an existing high low container
   */
  RoaringBitmap(RoaringArray highLowContainer) {
    this.highLowContainer = highLowContainer;
  }

  /**
   * Create a RoaringBitmap from a MutableRoaringBitmap or ImmutableRoaringBitmap. The source is not
   * modified.
   *
   * @param rb the original bitmap
   */
  public RoaringBitmap(ImmutableRoaringBitmap rb) {
    highLowContainer = new RoaringArray();
    MappeableContainerPointer cp = rb.getContainerPointer();
    while (cp.getContainer() != null) {
      highLowContainer.append(cp.key(), cp.getContainer().toContainer());
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
    final int i = highLowContainer.getIndex(hb);
    if (i >= 0) {
      highLowContainer.setContainerAtIndex(i,
          highLowContainer.getContainerAtIndex(i).add(Util.lowbits(x)));
    } else {
      final ArrayContainer newac = new ArrayContainer();
      highLowContainer.insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
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
      final int i = highLowContainer.getIndex((char) hb);

      if (i >= 0) {
        final Container c =
            highLowContainer.getContainerAtIndex(i).iadd(containerStart, containerLast + 1);
        highLowContainer.setContainerAtIndex(i, c);
      } else {
        highLowContainer.insertNewKeyValueAt(-i - 1, (char) hb,
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
    if (supremum <= minimum) {
      return false;
    }
    int minKey = (int)(minimum >>> 16);
    int supKey = (int)(supremum >>> 16);
    int length = highLowContainer.size;
    char[] keys = highLowContainer.keys;
    int offset = lowbitsAsInteger(minimum);
    int limit = lowbitsAsInteger(supremum);
    int index = Util.unsignedBinarySearch(keys, 0, length, (char)minKey);
    int pos = index >= 0 ? index : -index - 1;
    if (pos < length && supKey == (keys[pos])) {
      if (supKey > minKey) {
        offset = 0;
      }
      return highLowContainer.getContainerAtIndex(pos).intersects(offset, limit);
    }
    while (pos < length && supKey > (keys[pos])) {
      Container container = highLowContainer.getContainerAtIndex(pos);
      if (container.intersects(offset, 1 << 16)) {
        return true;
      }
      offset = 0;
      ++pos;
    }
    return pos < length && supKey == keys[pos]
            && highLowContainer.getContainerAtIndex(pos)
            .intersects(offset, limit);
  }


  /**
   * In-place bitwise AND (intersection) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void and(final RoaringBitmap x2) {
    int pos1 = 0, pos2 = 0, intersectionSize = 0;
    final int length1 = highLowContainer.size(), length2 = x2.highLowContainer.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = highLowContainer.getContainerAtIndex(pos1);
        final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        final Container c = c1.iand(c2);
        if (!c.isEmpty()) {
          highLowContainer.replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        pos1 = highLowContainer.advanceUntil(s2, pos1);
      } else { 
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    highLowContainer.resize(intersectionSize);
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
    final int length1 = highLowContainer.size(), length2 = x2.highLowContainer.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final Container c1 = highLowContainer.getContainerAtIndex(pos1);
        final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        final Container c = c1.iandNot(c2);
        if (!c.isEmpty()) {
          highLowContainer.replaceKeyAndContainerAtIndex(intersectionSize++, s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) { 
        if (pos1 != intersectionSize) {
          final Container c1 = highLowContainer.getContainerAtIndex(pos1);
          highLowContainer.replaceKeyAndContainerAtIndex(intersectionSize, s1, c1);
        }
        ++intersectionSize;
        ++pos1;
      } else { 
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

  /**
   * In-place bitwise ORNOT operation. The current bitmap is modified.
   *
   * @param other the other bitmap
   * @param rangeEnd end point of the range (exclusive).
   */
  public void orNot(final RoaringBitmap other, long rangeEnd) {
    rangeSanityCheck(0, rangeEnd);
    int maxKey = (int)((rangeEnd - 1) >>> 16);
    int lastRun = (rangeEnd & 0xFFFF) == 0 ? 0x10000 : (int)(rangeEnd & 0xFFFF);
    int size = 0;
    int pos1 = 0, pos2 = 0;
    int length1 = highLowContainer.size(), length2 = other.highLowContainer.size();
    int s1 = length1 > 0 ? highLowContainer.getKeyAtIndex(pos1) : maxKey + 1;
    int s2 = length2 > 0 ? other.highLowContainer.getKeyAtIndex(pos2) : maxKey + 1;
    int remainder = 0;
    for (int i = highLowContainer.size - 1;
         i >= 0 && highLowContainer.keys[i] > maxKey;
         --i) {
      ++remainder;
    }
    int correction = 0;
    for (int i = 0; i < other.highLowContainer.size - remainder; ++i) {
      correction += other.highLowContainer.getContainerAtIndex(i).isFull() ? 1 : 0;
      if (other.highLowContainer.getKeyAtIndex(i) >= maxKey) {
        break;
      }
    }
    // it's almost certain that the bitmap will grow, so make a conservative overestimate,
    // this avoids temporary allocation, and can trim afterwards
    int maxSize = Math.min(maxKey + 1 + remainder - correction + highLowContainer.size, 0x10000);
    if (maxSize == 0) {
      return;
    }
    char[] newKeys = new char[maxSize];
    Container[] newValues = new Container[maxSize];
    for (int key = 0; key <= maxKey && size < maxSize; ++key) {
      if (key == s1 && key == s2) { // actually need to do an or not
        newValues[size] = highLowContainer.getContainerAtIndex(pos1)
                .iorNot(other.highLowContainer.getContainerAtIndex(pos2),
                        key == maxKey ? lastRun : 0x10000);
        ++pos1;
        ++pos2;
        s1 = pos1 < length1 ? highLowContainer.getKeyAtIndex(pos1) : maxKey + 1;
        s2 = pos2 < length2 ? other.highLowContainer.getKeyAtIndex(pos2) : maxKey + 1;
      } else if (key == s1) { // or in a hole
        newValues[size] = highLowContainer.getContainerAtIndex(pos1)
                .ior(key == maxKey
                  ? RunContainer.rangeOfOnes(0, lastRun)
                  : RunContainer.full());
        ++pos1;
        s1 = pos1 < length1 ? highLowContainer.getKeyAtIndex(pos1) : maxKey + 1;
      } else if (key == s2) { // insert the complement
        newValues[size] = other.highLowContainer.getContainerAtIndex(pos2)
                .not(0, key == maxKey ? lastRun : 0x10000);
        ++pos2;
        s2 = pos2 < length2 ? other.highLowContainer.getKeyAtIndex(pos2) : maxKey + 1;
      } else { // key missing from both
        newValues[size] = key == maxKey
                        ? RunContainer.rangeOfOnes(0, lastRun)
                        : RunContainer.full();
      }
      // might have appended an empty container (rare case)
      if (newValues[size].isEmpty()) {
        newValues[size] = null;
      } else {
        newKeys[size] = (char)key;
        ++size;
      }
    }
    // copy over everything which will remain without being complemented
    if (remainder > 0) {
      System.arraycopy(highLowContainer.keys, highLowContainer.size - remainder,
              newKeys, size, remainder);
      System.arraycopy(highLowContainer.values, highLowContainer.size - remainder,
              newValues, size, remainder);
    }
    highLowContainer.keys = newKeys;
    highLowContainer.values = newValues;
    highLowContainer.size = size + remainder;
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
    int maxKey = (int)((rangeEnd - 1) >>> 16);
    int lastRun = (rangeEnd & 0xFFFF) == 0 ? 0x10000 : (int)(rangeEnd & 0xFFFF);
    int size = 0;
    int pos1 = 0, pos2 = 0;
    int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
    int s1 = length1 > 0 ? x1.highLowContainer.getKeyAtIndex(pos1) : maxKey + 1;
    int s2 = length2 > 0 ? x2.highLowContainer.getKeyAtIndex(pos2) : maxKey + 1;
    int remainder = 0;
    for (int i = x1.highLowContainer.size - 1;
         i >= 0 && x1.highLowContainer.keys[i] > maxKey;
         --i) {
      ++remainder;
    }
    int correction = 0;
    for (int i = 0; i < x2.highLowContainer.size - remainder; ++i) {
      correction += x2.highLowContainer.getContainerAtIndex(i).isFull() ? 1 : 0;
      if (x2.highLowContainer.getKeyAtIndex(i) >= maxKey) {
        break;
      }
    }
    // it's almost certain that the bitmap will grow, so make a conservative overestimate,
    // this avoids temporary allocation, and can trim afterwards
    int maxSize = Math.min(maxKey + 1 + remainder - correction + x1.highLowContainer.size, 0x10000);
    if (maxSize == 0) {
      return new RoaringBitmap();
    }
    char[] newKeys = new char[maxSize];
    Container[] newValues = new Container[maxSize];
    for (int key = 0; key <= maxKey && size < maxSize; ++key) {
      if (key == s1 && key == s2) { // actually need to do an or not
        newValues[size] = x1.highLowContainer.getContainerAtIndex(pos1)
                .orNot(x2.highLowContainer.getContainerAtIndex(pos2),
                        key == maxKey ? lastRun : 0x10000);
        ++pos1;
        ++pos2;
        s1 = pos1 < length1 ? x1.highLowContainer.getKeyAtIndex(pos1) : maxKey + 1;
        s2 = pos2 < length2 ? x2.highLowContainer.getKeyAtIndex(pos2) : maxKey + 1;
      } else if (key == s1) { // or in a hole
        newValues[size] = x1.highLowContainer.getContainerAtIndex(pos1)
                .ior(key == maxKey
                        ? RunContainer.rangeOfOnes(0, lastRun)
                        : RunContainer.full());
        ++pos1;
        s1 = pos1 < length1 ? x1.highLowContainer.getKeyAtIndex(pos1) : maxKey + 1;
      } else if (key == s2) { // insert the complement
        newValues[size] = x2.highLowContainer.getContainerAtIndex(pos2)
                .not(0, key == maxKey ? lastRun : 0x10000);
        ++pos2;
        s2 = pos2 < length2 ? x2.highLowContainer.getKeyAtIndex(pos2) : maxKey + 1;
      } else { // key missing from both
        newValues[size] = key == maxKey
                ? RunContainer.rangeOfOnes(0, lastRun)
                : RunContainer.full();
      }
      // might have appended an empty container (rare case)
      if (newValues[size].isEmpty()) {
        newValues[size] = null;
      } else {
        newKeys[size++] = (char)key;
      }
    }
    // copy over everything which will remain without being complemented
    if (remainder > 0) {
      System.arraycopy(x1.highLowContainer.keys, x1.highLowContainer.size - remainder,
              newKeys, size, remainder);
      System.arraycopy(x1.highLowContainer.values, x1.highLowContainer.size - remainder,
              newValues, size, remainder);
      for (int i = size; i < size + remainder; ++i) {
        newValues[i] = newValues[i].clone();
      }
    }
    RoaringBitmap result = new RoaringBitmap();
    result.highLowContainer.keys = newKeys;
    result.highLowContainer.values = newValues;
    result.highLowContainer.size = size + remainder;
    return result;
  }



  /**
   * Add the value to the container (set the value to "true"), whether it already appears or not.
   *
   * @param x integer value
   * @return true if the added int wasn't already contained in the bitmap. False otherwise.
   */
  public boolean checkedAdd(final int x) {
    final char hb = Util.highbits(x);
    final int i = highLowContainer.getIndex(hb);
    if (i >= 0) {
      Container c = highLowContainer.getContainerAtIndex(i);
      int oldCard = c.getCardinality();
      // we need to keep the newContainer if a switch between containers type
      // occur, in order to get the new cardinality
      Container newCont = c.add(Util.lowbits(x));
      highLowContainer.setContainerAtIndex(i, newCont);
      if (newCont.getCardinality() > oldCard) {
        return true;
      }
    } else {
      final ArrayContainer newac = new ArrayContainer();
      highLowContainer.insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
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
    final int i = highLowContainer.getIndex(hb);
    if (i < 0) {
      return false;
    }
    Container C = highLowContainer.getContainerAtIndex(i);
    int oldcard = C.getCardinality();
    C.remove(Util.lowbits(x));
    int newcard = C.getCardinality();
    if (newcard == oldcard) {
      return false;
    }
    if (newcard > 0) {
      highLowContainer.setContainerAtIndex(i, C);
    } else {
      highLowContainer.removeAtIndex(i);
    }
    return true;
  }

  /**
   * reset to an empty bitmap; result occupies as much space a newly created bitmap.
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
   * Checks whether the value is included, which is equivalent to checking if the corresponding bit
   * is set (get in BitSet class).
   *
   * @param x integer value
   * @return whether the integer value is included.
   */
  @Override
  public boolean contains(final int x) {
    final char hb = Util.highbits(x);
    final Container c = highLowContainer.getContainer(hb);
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
    if (supremum <= minimum) {
      return false;
    }
    char firstKey = Util.highbits(minimum);
    char lastKey = Util.highbits(supremum);
    int span = lastKey - firstKey;
    int len = highLowContainer.size;
    if (len < span) {
      return false;
    }
    int begin = highLowContainer.getIndex(firstKey);
    int end = highLowContainer.getIndex(lastKey);
    end = end < 0 ? -end -1 : end;
    if (begin < 0 || end - begin != span) {
      return false;
    }

    int min = (char)minimum;
    int sup = (char)supremum;
    if (firstKey == lastKey) {
      return highLowContainer.getContainerAtIndex(begin)
              .contains(min, (supremum & 0xFFFF) == 0 ? 0x10000 : sup);
    }
    if (!highLowContainer.getContainerAtIndex(begin).contains(min, 1 << 16)) {
      return false;
    }
    if (end < len && !highLowContainer.getContainerAtIndex(end)
            .contains(0, (supremum & 0xFFFF) == 0 ? 0x10000 : sup)) {
      return false;
    }
    for (int i = begin + 1; i < end; ++i) {
      if (highLowContainer.getContainerAtIndex(i).getCardinality() != 1 << 16) {
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
      this.highLowContainer.deserialize(in, buffer);
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
      this.highLowContainer.deserialize(in);
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
      this.highLowContainer.deserialize(bbf);
    } catch(InvalidRoaringFormat cookie) {
      throw cookie.toIOException();// we convert it to an IOException
    }
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
   * Returns true if the other bitmap has no more than tolerance bits
   * differing from this bitmap. The other may be transformed into a bitmap equal
   * to this bitmap in no more than tolerance bit flips if this method returns true.
   *
   * @param other the bitmap to compare to
   * @param tolerance the maximum number of bits that may differ
   * @return true if the number of differing bits is smaller than tolerance
   */
  public boolean isHammingSimilar(RoaringBitmap other, int tolerance) {
    final int size1 = highLowContainer.size();
    final int size2 = other.highLowContainer.size();
    int pos1 = 0;
    int pos2 = 0;
    int budget = tolerance;
    while(budget >= 0 && pos1 < size1 && pos2 < size2) {
      final char key1 = this.highLowContainer.getKeyAtIndex(pos1);
      final char key2 = other.highLowContainer.getKeyAtIndex(pos2);
      Container left = highLowContainer.getContainerAtIndex(pos1);
      Container right = other.highLowContainer.getContainerAtIndex(pos2);
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
      Container container = highLowContainer.getContainerAtIndex(pos1++);
      budget -= container.getCardinality();
    }
    while(budget >= 0 && pos2 < size2) {
      Container container = other.highLowContainer.getContainerAtIndex(pos2++);
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
    final int i = highLowContainer.getIndex(hb);
    if (i >= 0) {
      Container c = highLowContainer.getContainerAtIndex(i).flip(Util.lowbits(x));
      if (!c.isEmpty()) {
        highLowContainer.setContainerAtIndex(i, c);
      } else {
        highLowContainer.removeAtIndex(i);
      }
    } else {
      final ArrayContainer newac = new ArrayContainer();
      highLowContainer.insertNewKeyValueAt(-i - 1, hb, newac.add(Util.lowbits(x)));
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
      final int i = highLowContainer.getIndex((char) hb);

      if (i >= 0) {
        final Container c =
            highLowContainer.getContainerAtIndex(i).inot(containerStart, containerLast + 1);
        if (!c.isEmpty()) {
          highLowContainer.setContainerAtIndex(i, c);
        } else {
          highLowContainer.removeAtIndex(i);
        }
      } else {
        highLowContainer.insertNewKeyValueAt(-i - 1, (char) hb,
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
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      size += this.highLowContainer.getContainerAtIndex(i).getCardinality();
    }
    return size;
  }

  @Override
  public int getCardinality() {
    return (int) getLongCardinality();
  }

  @Override
  public void forEach(IntConsumer ic) {
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      this.highLowContainer.getContainerAtIndex(i).forEach(this.highLowContainer.keys[i], ic);
    }
  }


  /**
   * Return a low-level container pointer that can be used to access the underlying data structure.
   *
   * @return container pointer
   */
  public ContainerPointer getContainerPointer() {
    return this.highLowContainer.getContainerPointer();
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
    return new RoaringBatchIterator(highLowContainer);
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
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      final Container c = this.highLowContainer.getContainerAtIndex(i);
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

  @Override
  public int hashCode() {
    return highLowContainer.hashCode();
  }


  /**
   * Check whether this bitmap has had its runs compressed.
   *
   * @return whether this bitmap has run compression
   */
  public boolean hasRunCompression() {
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      Container c = this.highLowContainer.getContainerAtIndex(i);
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
    return highLowContainer.size() == 0;
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
        return pos < RoaringBitmap.this.highLowContainer.size();
      }

      private Iterator<Integer> init() {
        if (pos < RoaringBitmap.this.highLowContainer.size()) {
          iter = RoaringBitmap.this.highLowContainer.getContainerAtIndex(pos).getCharIterator();
          hs = RoaringBitmap.this.highLowContainer.getKeyAtIndex(pos) << 16;
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
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = highLowContainer.getKeyAtIndex(pos1);
      char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          this.highLowContainer.setContainerAtIndex(pos1, highLowContainer.getContainerAtIndex(pos1)
              .lazyIOR(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { 
          highLowContainer.insertNewKeyValueAt(pos1, s2,
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
      highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
    }
  }

  // don't forget to call repairAfterLazy() afterward
  // important: x2 should not have been computed lazily
  // this method is like lazyor except that it will convert
  // the current container to a bitset
  protected void naivelazyor(RoaringBitmap  x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = highLowContainer.getKeyAtIndex(pos1);
      char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          BitmapContainer c1 = highLowContainer.getContainerAtIndex(pos1).toBitmapContainer();
          this.highLowContainer.setContainerAtIndex(pos1,
              c1.lazyIOR(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { 
          highLowContainer.insertNewKeyValueAt(pos1, s2,
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
      highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
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
        && (i < this.highLowContainer.size()); i++) {
      Container c = this.highLowContainer.getContainerAtIndex(i);
      if (c.getCardinality() + currentcardinality <= maxcardinality) {
        answer.highLowContainer.appendCopy(this.highLowContainer, i);
        currentcardinality += c.getCardinality();
      } else {
        int leftover = maxcardinality - currentcardinality;
        Container limited = c.limit(leftover);
        answer.highLowContainer.append(this.highLowContainer.getKeyAtIndex(i), limited);
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
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();
    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = highLowContainer.getKeyAtIndex(pos1);
      char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          this.highLowContainer.setContainerAtIndex(pos1, highLowContainer.getContainerAtIndex(pos1)
              .ior(x2.highLowContainer.getContainerAtIndex(pos2)));
          pos1++;
          pos2++;
          if ((pos1 == length1) || (pos2 == length2)) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
          s2 = x2.highLowContainer.getKeyAtIndex(pos2);
        } else if (s1 < s2) { 
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { 
          highLowContainer.insertNewKeyValueAt(pos1, s2,
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
      highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
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
   * GetCardinality()).  If you provide the smallest value as a parameter, this function will
   * return 1. If provide a value smaller than the smallest value, it will return 0.
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

    for (int i = 0; i < this.highLowContainer.size(); i++) {
      char key = this.highLowContainer.getKeyAtIndex(i);
      if (key < xhigh) {
        size += this.highLowContainer.getContainerAtIndex(i).getCardinality();
      } else if (key == xhigh) {
        return size + this.highLowContainer.getContainerAtIndex(i).rank(Util.lowbits(x));
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
    int startIndex = this.highLowContainer.getIndex(Util.highbits(start));
    if(startIndex < 0)  {
      startIndex = -startIndex - 1;
    } else {
      int inContainerStart = (Util.lowbits(start));
      if(inContainerStart != 0) {
        size -= this.highLowContainer
          .getContainerAtIndex(startIndex)
          .rank((char)(inContainerStart - 1));
      }
    }
    char xhigh = Util.highbits(end - 1);
    for (int i = startIndex; i < this.highLowContainer.size(); i++) {
      char key = this.highLowContainer.getKeyAtIndex(i);
      if (key < xhigh) {
        size += this.highLowContainer.getContainerAtIndex(i).getCardinality();
      } else if (key == xhigh) {
        return size + this.highLowContainer
          .getContainerAtIndex(i).rank(Util.lowbits((int)(end - 1)));
      }
    }
    return size;
  }



  @Override
  public int rank(int x) {
    return (int) rankLong(x);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    this.highLowContainer.readExternal(in);
  }

  /**
   * If present remove the specified integer (effectively, sets its bit value to false)
   *
   * @param x integer value representing the index in a bitmap
   */
  @Override
  public void remove(final int x) {
    final char hb = Util.highbits(x);
    final int i = highLowContainer.getIndex(hb);
    if (i < 0) {
      return;
    }
    highLowContainer.setContainerAtIndex(i,
        highLowContainer.getContainerAtIndex(i).remove(Util.lowbits(x)));
    if (highLowContainer.getContainerAtIndex(i).isEmpty()) {
      highLowContainer.removeAtIndex(i);
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
      final int i = highLowContainer.getIndex((char) hbStart);
      if (i < 0) {
        return;
      }
      final Container c = highLowContainer.getContainerAtIndex(i).iremove(lbStart, lbLast + 1);
      if (!c.isEmpty()) {
        highLowContainer.setContainerAtIndex(i, c);
      } else {
        highLowContainer.removeAtIndex(i);
      }
      return;
    }
    int ifirst = highLowContainer.getIndex((char) hbStart);
    int ilast = highLowContainer.getIndex((char) hbLast);
    if (ifirst >= 0) {
      if (lbStart != 0) {
        final Container c = highLowContainer.getContainerAtIndex(ifirst).iremove(lbStart,
            Util.maxLowBitAsInteger() + 1);
        if (!c.isEmpty()) {
          highLowContainer.setContainerAtIndex(ifirst, c);
          ifirst++;
        }
      }
    } else {
      ifirst = -ifirst - 1;
    }
    if (ilast >= 0) {
      if (lbLast != Util.maxLowBitAsInteger()) {
        final Container c = highLowContainer.getContainerAtIndex(ilast).iremove(0, lbLast + 1);
        if (!c.isEmpty()) {
          highLowContainer.setContainerAtIndex(ilast, c);
        } else {
          ilast++;
        }
      } else {
        ilast++;
      }
    } else {
      ilast = -ilast - 1;
    }
    highLowContainer.removeIndexRange(ifirst, ilast);
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
      Container c = this.highLowContainer.getContainerAtIndex(i);
      if (c instanceof RunContainer) {
        Container newc = ((RunContainer) c).toBitmapOrArrayContainer(c.getCardinality());
        this.highLowContainer.setContainerAtIndex(i, newc);
        answer = true;
      }
    }
    return answer;
  }

  // to be used with lazyor
  protected void repairAfterLazy() {
    for (int k = 0; k < highLowContainer.size(); ++k) {
      Container c = highLowContainer.getContainerAtIndex(k);
      highLowContainer.setContainerAtIndex(k, c.repairAfterLazy());
    }
  }

  /**
   * Use a run-length encoding where it is more space efficient
   *
   * @return whether a change was applied
   */
  public boolean runOptimize() {
    boolean answer = false;
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      Container c = this.highLowContainer.getContainerAtIndex(i).runOptimize();
      if (c instanceof RunContainer) {
        answer = true;
      }
      this.highLowContainer.setContainerAtIndex(i, c);
    }
    return answer;
  }

  /**
   * Checks whether the parameter is a subset of this RoaringBitmap or not
   * @param subset the potential subset
   * @return true if the parameter is a subset of this RoaringBitmap
   */
  public boolean contains(RoaringBitmap subset) {
    final int length1 = this.highLowContainer.size;
    final int length2 = subset.highLowContainer.size;
    int pos1 = 0, pos2 = 0;
    while (pos1 < length1 && pos2 < length2) {
      final char s1 = this.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = subset.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        Container c1 = this.highLowContainer.getContainerAtIndex(pos1);
        Container c2 = subset.highLowContainer.getContainerAtIndex(pos2);
        if(!c1.contains(c2)) {
          return false;
        }
        ++pos1;
        ++pos2;
      } else if (s1 - s2 > 0) {
        return false;
      } else {
        pos1 = subset.highLowContainer.advanceUntil(s2, pos1);
      }
    }
    return pos2 == length2;
  }


  /**
   * Return the jth value stored in this bitmap. The provided value
   * needs to be smaller than the cardinality otherwise an
   * IllegalArgumentException
   * exception is thrown.  The smallest value is at index 0.
   * Note that this function differs in convention from the rank function which
   * returns 1 when ranking the smallest value.
   *
   * @param j index of the value
   *
   * @return the value
   * @see <a href="https://en.wikipedia.org/wiki/Selection_algorithm">Selection algorithm</a>
   */
  @Override
  public int select(int j) {
    long leftover = Util.toUnsignedLong(j);
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      Container c = this.highLowContainer.getContainerAtIndex(i);
      int thiscard = c.getCardinality();
      if (thiscard > leftover) {
        int keycontrib = this.highLowContainer.getKeyAtIndex(i) << 16;
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
    int containerIndex = highLowContainer.advanceUntil(key, -1);
    long nextSetBit = -1L;
    while (containerIndex < highLowContainer.size() && nextSetBit == -1L) {
      char containerKey = highLowContainer.getKeyAtIndex(containerIndex);
      Container container = highLowContainer.getContainerAtIndex(containerIndex);
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
    int containerIndex = highLowContainer.advanceUntil(key, -1);
    if (containerIndex == highLowContainer.size()) {
      return last();
    }
    long prevSetBit = -1L;
    while (containerIndex != -1 && prevSetBit == -1L) {
      char containerKey = highLowContainer.getKeyAtIndex(containerIndex);
      Container container = highLowContainer.getContainerAtIndex(containerIndex);
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
    int containerIndex = highLowContainer.advanceUntil(key, -1);

    int size = highLowContainer.size();
    if (containerIndex == size) {
      return Util.toUnsignedLong(fromValue);
    }
    char containerKey = highLowContainer.getKeyAtIndex(containerIndex);
    if (fromValue < containerKey << 16) {
      return Util.toUnsignedLong(fromValue);
    }
    Container container = highLowContainer.getContainerAtIndex(containerIndex);
    int bit = container.nextAbsentValue(Util.lowbits(fromValue));
    while (true) {
      if (bit != 1 << 16) {
        return Util.toUnsignedLong((containerKey << 16) | bit);
      }
      assert container.last() == (1 << 16) - 1;
      if (containerIndex == size - 1) {
        return Util.toUnsignedLong(highLowContainer.last()) + 1;
      }

      containerIndex += 1;
      char nextContainerKey = highLowContainer.getKeyAtIndex(containerIndex);
      if (containerKey + 1 < nextContainerKey) {
        return Util.toUnsignedLong((containerKey + 1) << 16);
      }
      containerKey = nextContainerKey;
      container = highLowContainer.getContainerAtIndex(containerIndex);
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
    int containerIndex = highLowContainer.advanceUntil(key, -1);

    if (containerIndex == highLowContainer.size()) {
      return Util.toUnsignedLong(fromValue);
    }
    char containerKey = highLowContainer.getKeyAtIndex(containerIndex);
    if (fromValue < containerKey << 16) {
      return Util.toUnsignedLong(fromValue);
    }
    Container container = highLowContainer.getContainerAtIndex(containerIndex);
    int bit = container.previousAbsentValue(Util.lowbits(fromValue));

    while (true) {
      if (bit != -1) {
        return Util.toUnsignedLong((containerKey << 16) | bit);
      }
      assert container.first() == 0;
      if (containerIndex == 0) {
        return Util.toUnsignedLong(highLowContainer.first()) - 1;
      }

      containerIndex -= 1;
      char nextContainerKey = highLowContainer.getKeyAtIndex(containerIndex);
      if (nextContainerKey < containerKey - 1) {
        return Util.toUnsignedLong((containerKey << 16)) - 1;
      }
      containerKey = nextContainerKey;
      container = highLowContainer.getContainerAtIndex(containerIndex);
      bit = container.previousAbsentValue((char) ((1 << 16) - 1));
    }
  }

  /**
   * Get the first (smallest) integer in this RoaringBitmap,
   * that is, returns the minimum of the set.
   * @return the first (smallest) integer
   * @throws NoSuchElementException if empty
   */
  @Override
  public int first() {
    return highLowContainer.first();
  }

  /**
   * Get the last (largest) integer in this RoaringBitmap,
   * that is, returns the maximum of the set.
   * @return the last (largest) integer
   * @throws NoSuchElementException if empty
   */
  @Override
  public int last() {
    return highLowContainer.last();
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
    this.highLowContainer.serialize(out);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    highLowContainer.serialize(buffer);
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
   * Report the number of bytes required to serialize this bitmap. This is the number of bytes
   * written out when using the serialize method. When using the writeExternal method, the count
   * will be higher due to the overhead of Java serialization.
   *
   * @return the size in bytes
   */
  @Override
  public int serializedSizeInBytes() {
    return this.highLowContainer.serializedSizeInBytes();
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
      final int i = rb.highLowContainer.getIndex((char) hbStart);
      if (i >= 0) {
        final Container c = rb.highLowContainer.getContainerAtIndex(i).remove(0, lbStart)
            .iremove(lbLast + 1, Util.maxLowBitAsInteger() + 1);
        if (!c.isEmpty()) {
          answer.highLowContainer.append((char) hbStart, c);
        }
      }
      return answer;
    }
    int ifirst = rb.highLowContainer.getIndex((char) hbStart);
    int ilast = rb.highLowContainer.getIndex((char) hbLast);
    if (ifirst >= 0) {
      final Container c = rb.highLowContainer.getContainerAtIndex(ifirst).remove(0, lbStart);
      if (!c.isEmpty()) {
        answer.highLowContainer.append((char) hbStart, c);
      }
    }

    // revised to loop on ints
    for (int hb = hbStart + 1; hb <= hbLast - 1; ++hb) {
      final int i = rb.highLowContainer.getIndex((char)hb);
      final int j = answer.highLowContainer.getIndex((char) hb);
      assert j < 0;

      if (i >= 0) {
        final Container c = rb.highLowContainer.getContainerAtIndex(i);
        answer.highLowContainer.insertNewKeyValueAt(-j - 1, (char)hb, c);
      }
    }

    if (ilast >= 0) {
      final Container c = rb.highLowContainer.getContainerAtIndex(ilast).remove(lbLast + 1,
          Util.maxLowBitAsInteger() + 1);
      if (!c.isEmpty()) {
        answer.highLowContainer.append((char) hbLast, c);
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
    while (pos < this.highLowContainer.size()) {
      final int hs = this.highLowContainer.getKeyAtIndex(pos) << 16;
      Container c = this.highLowContainer.getContainerAtIndex(pos++);
      c.fillLeastSignificant16bits(array, pos2, hs);
      pos2 += c.getCardinality();
    }
    return array;
  }

  @Override
  public void append(char key, Container container) {
    highLowContainer.append(key, container);
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
   * Recover allocated but unused memory.
   */
  @Override
  public void trim() {
    this.highLowContainer.trim();
  }


  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    this.highLowContainer.writeExternal(out);
  }

  /**
   * In-place bitwise XOR (symmetric difference) operation. The current bitmap is modified.
   *
   * @param x2 other bitmap
   */
  public void xor(final RoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    int length1 = highLowContainer.size();
    final int length2 = x2.highLowContainer.size();

    main: if (pos1 < length1 && pos2 < length2) {
      char s1 = highLowContainer.getKeyAtIndex(pos1);
      char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      while (true) {
        if (s1 == s2) {
          final Container c = highLowContainer.getContainerAtIndex(pos1)
              .ixor(x2.highLowContainer.getContainerAtIndex(pos2));
          if (!c.isEmpty()) {
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
        } else if (s1 < s2) { 
          pos1++;
          if (pos1 == length1) {
            break main;
          }
          s1 = highLowContainer.getKeyAtIndex(pos1);
        } else { 
          highLowContainer.insertNewKeyValueAt(pos1, s2,
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
      highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
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
