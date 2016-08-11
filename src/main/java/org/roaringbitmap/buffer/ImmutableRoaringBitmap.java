/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.PeekableShortIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.ShortIterator;
import org.roaringbitmap.Util;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * ImmutableRoaringBitmap provides a compressed immutable (cannot be modified) bitmap. It is meant
 * to be used with org.roaringbitmap.buffer.MutableRoaringBitmap, a derived class that adds methods
 * to modify the bitmap.
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
 * 
 * @see MutableRoaringBitmap
 */
public class ImmutableRoaringBitmap
    implements Iterable<Integer>, Cloneable, ImmutableBitmapDataProvider {

  private final class ImmutableRoaringIntIterator implements PeekableIntIterator {
    private MappeableContainerPointer cp =
        ImmutableRoaringBitmap.this.highLowContainer.getContainerPointer();

    private int hs = 0;

    private PeekableShortIterator iter;

    private boolean ok;

    public ImmutableRoaringIntIterator() {
      nextContainer();
    }

    @Override
    public PeekableIntIterator clone() {
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
    public boolean hasNext() {
      return ok;
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


    private void nextContainer() {
      ok = cp.hasContainer();
      if (ok) {
        iter = cp.getContainer().getShortIterator();
        hs = BufferUtil.toIntUnsigned(cp.key()) << 16;
      }
    }

    @Override
    public void advanceIfNeeded(int minval) {
      while (hasNext() && ((hs >>> 16) < (minval >>> 16))) {
        cp.advance();
        nextContainer();
      }
      if (ok && ((hs >>> 16) == (minval >>> 16))) {
        iter.advanceIfNeeded(BufferUtil.lowbits(minval));
        if (!iter.hasNext()) {
          cp.advance();
          nextContainer();
        }
      }
    }

    @Override
    public int peekNext() {
      return BufferUtil.toIntUnsigned(iter.peekNext()) | hs;
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
    public IntIterator clone() {
      try {
        ImmutableRoaringReverseIntIterator x = (ImmutableRoaringReverseIntIterator) super.clone();
        x.iter = this.iter.clone();
        x.cp = this.cp.clone();
        return x;
      } catch (CloneNotSupportedException e) {
        return null;// will not happen
      }
    }

    @Override
    public boolean hasNext() {
      return ok;
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


    private void nextContainer() {
      ok = cp.hasContainer();
      if (ok) {
        iter = cp.getContainer().getReverseShortIterator();
        hs = BufferUtil.toIntUnsigned(cp.key()) << 16;
      }
    }


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
  public static MutableRoaringBitmap and(@SuppressWarnings("rawtypes") final Iterator bitmaps,
      final long rangeStart, final long rangeEnd) {
    MutableRoaringBitmap.rangeSanityCheck(rangeStart,rangeEnd);
    Iterator<ImmutableRoaringBitmap> bitmapsIterator;
    bitmapsIterator = selectRangeWithoutCopy(bitmaps, rangeStart, rangeEnd);
    return BufferFastAggregation.and(bitmapsIterator);
  }

  /**
   * 
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
    public static MutableRoaringBitmap and(@SuppressWarnings("rawtypes") final Iterator bitmaps, 
      final int rangeStart, final int rangeEnd) {
    return and(bitmaps, (long) rangeStart, (long) rangeEnd);
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
        pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
      } else { // s1 > s2
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
   * @see BufferFastAggregation#and(ImmutableRoaringBitmap...)
   */
  public static int andCardinality(final ImmutableRoaringBitmap x1,
      final ImmutableRoaringBitmap x2) {
    int answer = 0;
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    while (pos1 < length1 && pos2 < length2) {
      final short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      if (s1 == s2) {
        final MappeableContainer c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        answer += c1.andCardinality(c2);
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
   * Bitwise ANDNOT (difference) operation for the given range, rangeStart (inclusive) and rangeEnd
   * (exclusive). The provided bitmaps are *not* modified. This operation is thread-safe as long as
   * the provided bitmaps remain unchanged.
   * 
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @param rangeStart beginning of the range (inclusive)
   * @param rangeEnd end of range (exclusive)
   * @return result of the operation
   */
  public static MutableRoaringBitmap andNot(final ImmutableRoaringBitmap x1,
      final ImmutableRoaringBitmap x2, long rangeStart, long rangeEnd) {
    MutableRoaringBitmap.rangeSanityCheck(rangeStart,rangeEnd);
    MutableRoaringBitmap rb1 = selectRangeWithoutCopy(x1, rangeStart, rangeEnd);
    MutableRoaringBitmap rb2 = selectRangeWithoutCopy(x2, rangeStart, rangeEnd);
    return andNot(rb1, rb2);
  }


  /**
   * Bitwise ANDNOT (difference) operation for the given range, rangeStart (inclusive) and rangeEnd
   * (exclusive). The provided bitmaps are *not* modified. This operation is thread-safe as long as
   * the provided bitmaps remain unchanged.
   * 
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @param rangeStart beginning of the range (inclusive)
   * @param rangeEnd end of range (exclusive)
   * @return result of the operation
   * @deprecated use the version where longs specify the range. Negative values for range
   *     endpoints are not allowed.
   */
  @Deprecated
    public static MutableRoaringBitmap andNot(final ImmutableRoaringBitmap x1, 
                                              final ImmutableRoaringBitmap x2, 
                                              final int rangeStart, final int rangeEnd) {
    return andNot(x1, x2, (long) rangeStart, (long) rangeEnd);
  }



  /**
   * Bitwise ANDNOT (difference) operation. The provided bitmaps are *not* modified. This operation
   * is thread-safe as long as the provided bitmaps remain unchanged.
   * 
   * @param x1 first bitmap
   * @param x2 other bitmap
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
   * Generate a bitmap with the specified values set to true. The provided integers values don't
   * have to be in sorted order, but it may be preferable to sort them from a performance point of
   * view.
   * 
   * This function is equivalent to :
   * 
   * <pre>
   * {@code
   *       (ImmutableRoaringBitmap) MutableRoaringBitmap.bitmapOf(data)
   * }
   * </pre>
   * 
   * @param data set values
   * @return a new bitmap
   */
  public static ImmutableRoaringBitmap bitmapOf(final int... data) {
    return MutableRoaringBitmap.bitmapOf(data);
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
  public static MutableRoaringBitmap flip(ImmutableRoaringBitmap bm, final long rangeStart,
      final long rangeEnd) {
    MutableRoaringBitmap.rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      throw new RuntimeException("Invalid range " + rangeStart + " -- " + rangeEnd);
    }

    MutableRoaringBitmap answer = new MutableRoaringBitmap();
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
        final MappeableContainer c =
            bm.highLowContainer.getContainerAtIndex(i).not(containerStart, containerLast + 1);
        if (c.getCardinality() > 0) {
          answer.getMappeableRoaringArray().insertNewKeyValueAt(-j - 1, hb, c);
        }

      } else { // *think* the range of ones must never be
        // empty.
        answer.getMappeableRoaringArray().insertNewKeyValueAt(-j - 1, hb,
            MappeableContainer.rangeOfOnes(containerStart, containerLast + 1));
      }
    }
    // copy the containers after the active area.
    answer.getMappeableRoaringArray().appendCopiesAfter(bm.highLowContainer, hbLast);

    return answer;
  }



 /**
   * Complements the bits in the given range, from rangeStart (inclusive) rangeEnd (exclusive). The
   * given bitmap is unchanged.
   * 
   * @param bm bitmap being negated
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return a new Bitmap
   * @deprecated use the version where longs specify the range
   */
  @Deprecated
    public static MutableRoaringBitmap flip(ImmutableRoaringBitmap bm,
                                            final int rangeStart, final int rangeEnd) {
    if (rangeStart >= 0) {
      return flip(bm, (long) rangeStart, (long) rangeEnd);
    }
    // rangeStart being -ve and rangeEnd being positive is not expected)
    // so assume both -ve
    return flip(bm, rangeStart & 0xFFFFFFFFL, rangeEnd & 0xFFFFFFFFL); 
  }



  /**
   * Return new iterator with only values from rangeStart (inclusive) to rangeEnd (exclusive)
   * 
   * @param input bitmaps iterator
   * @param rangeStart inclusive
   * @param rangeEnd exclusive
   * @return new iterator of bitmaps
   */
  private static Iterator<ImmutableRoaringBitmap> selectRangeWithoutCopy(final Iterator bitmaps,
      final long rangeStart, final long rangeEnd) {
    Iterator<ImmutableRoaringBitmap> bitmapsIterator;
    bitmapsIterator = new Iterator<ImmutableRoaringBitmap>() {
      @Override
      public boolean hasNext() {
        return bitmaps.hasNext();
      }

      @Override
      public ImmutableRoaringBitmap next() {
        ImmutableRoaringBitmap next = (ImmutableRoaringBitmap) bitmaps.next();
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

  private static MutableRoaringBitmap selectRangeWithoutCopy(ImmutableRoaringBitmap rb,
      final long rangeStart, final long rangeEnd) {
    final int hbStart = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeStart));
    final int lbStart = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeStart));
    final int hbLast = BufferUtil.toIntUnsigned(BufferUtil.highbits(rangeEnd - 1));
    final int lbLast = BufferUtil.toIntUnsigned(BufferUtil.lowbits(rangeEnd - 1));
    MutableRoaringBitmap answer = new MutableRoaringBitmap();

    if (rangeEnd <= rangeStart) {
      return answer;
    }

    if (hbStart == hbLast) {
      final int i = rb.highLowContainer.getIndex((short) hbStart);
      if (i >= 0) {
        final MappeableContainer c = rb.highLowContainer.getContainerAtIndex(i).remove(0, lbStart)
            .iremove(lbLast + 1, BufferUtil.maxLowBitAsInteger() + 1);
        if (c.getCardinality() > 0) {
          ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
        }
      }
      return answer;
    }
    int ifirst = rb.highLowContainer.getIndex((short) hbStart);
    int ilast = rb.highLowContainer.getIndex((short) hbLast);
    if (ifirst >= 0) {
      final MappeableContainer c =
          rb.highLowContainer.getContainerAtIndex(ifirst).remove(0, lbStart);
      if (c.getCardinality() > 0) {
        ((MutableRoaringArray) answer.highLowContainer).append((short) hbStart, c);
      }
    }

    for (int hb = hbStart + 1; hb <= hbLast - 1; ++hb) {
      final int i = rb.highLowContainer.getIndex((short) hb);
      final int j = answer.getMappeableRoaringArray().getIndex((short) hb);
      assert j < 0;

      if (i >= 0) {
        final MappeableContainer c = rb.highLowContainer.getContainerAtIndex(i);
        answer.getMappeableRoaringArray().insertNewKeyValueAt(-j - 1, (short) hb, c);
      }
    }

    if (ilast >= 0) {
      final MappeableContainer c = rb.highLowContainer.getContainerAtIndex(ilast).remove(lbLast + 1,
          BufferUtil.maxLowBitAsInteger() + 1);
      if (c.getCardinality() > 0) {
        ((MutableRoaringArray) answer.highLowContainer).append((short) hbLast, c);
      }
    }
    return answer;

  }

  /**
   * Checks whether the two bitmaps intersect. This can be much faster than calling "and" and
   * checking the cardinality of the result.
   *
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return true if they intersect
   */
  public static boolean intersects(final ImmutableRoaringBitmap x1,
      final ImmutableRoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    while (pos1 < length1 && pos2 < length2) {
      final short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      if (s1 == s2) {
        final MappeableContainer c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        if (c1.intersects(c2)) {
          return true;
        }
        ++pos1;
        ++pos2;
      } else if (Util.compareUnsigned(s1, s2) < 0) { // s1 < s2
        pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
      } else { // s1 > s2
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    return false;
  }

  // important: inputs should not be reused
  protected static MutableRoaringBitmap lazyor(final ImmutableRoaringBitmap x1,
      final ImmutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
    MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();
    main: if (i1.hasContainer() && i2.hasContainer()) {
      while (true) {
        if (i1.key() == i2.key()) {
          answer.getMappeableRoaringArray().append(i1.key(),
              i1.getContainer().lazyOR(i2.getContainer()));
          i1.advance();
          i2.advance();
          if (!i1.hasContainer() || !i2.hasContainer()) {
            break main;
          }
        } else if (Util.compareUnsigned(i1.key(), i2.key()) < 0) { // i1.key() < i2.key()
          answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
          i1.advance();
          if (!i1.hasContainer()) {
            break main;
          }
        } else { // i1.key() > i2.key()
          answer.getMappeableRoaringArray().appendCopy(i2.key(), i2.getContainer());
          i2.advance();
          if (!i2.hasContainer()) {
            break main;
          }
        }
      }
    }
    if (!i1.hasContainer()) {
      while (i2.hasContainer()) {
        answer.getMappeableRoaringArray().appendCopy(i2.key(), i2.getContainer());
        i2.advance();
      }
    } else if (!i2.hasContainer()) {
      while (i1.hasContainer()) {
        answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
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
   * If you have more than 2 bitmaps, consider using the FastAggregation class.
   * 
   * @param x1 first bitmap
   * @param x2 other bitmap
   * @return result of the operation
   * @see BufferFastAggregation#or(ImmutableRoaringBitmap...)
   * @see BufferFastAggregation#horizontal_or(ImmutableRoaringBitmap...)
   */
  public static MutableRoaringBitmap or(final ImmutableRoaringBitmap x1,
      final ImmutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
    MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();
    main: if (i1.hasContainer() && i2.hasContainer()) {
      while (true) {
        if (i1.key() == i2.key()) {
          answer.getMappeableRoaringArray().append(i1.key(),
              i1.getContainer().or(i2.getContainer()));
          i1.advance();
          i2.advance();
          if (!i1.hasContainer() || !i2.hasContainer()) {
            break main;
          }
        } else if (Util.compareUnsigned(i1.key(), i2.key()) < 0) { // i1.key() < i2.key()
          answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
          i1.advance();
          if (!i1.hasContainer()) {
            break main;
          }
        } else { // i1.key() > i2.key()
          answer.getMappeableRoaringArray().appendCopy(i2.key(), i2.getContainer());
          i2.advance();
          if (!i2.hasContainer()) {
            break main;
          }
        }
      }
    }
    if (!i1.hasContainer()) {
      while (i2.hasContainer()) {
        answer.getMappeableRoaringArray().appendCopy(i2.key(), i2.getContainer());
        i2.advance();
      }
    } else if (!i2.hasContainer()) {
      while (i1.hasContainer()) {
        answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
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
   * @param bitmaps input bitmaps
   * @return aggregated bitmap
   */
  public static MutableRoaringBitmap or(@SuppressWarnings("rawtypes") Iterator bitmaps) {
    return BufferFastAggregation.or(bitmaps);
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
  public static MutableRoaringBitmap or(@SuppressWarnings("rawtypes") final Iterator bitmaps,
      final long rangeStart, final long rangeEnd) {
    MutableRoaringBitmap.rangeSanityCheck(rangeStart, rangeEnd);
    Iterator<ImmutableRoaringBitmap> bitmapsIterator;
    bitmapsIterator = selectRangeWithoutCopy(bitmaps, rangeStart, rangeEnd);
    return or(bitmapsIterator);
  }

  /**
   * Computes OR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   * @deprecated use the version where longs specify the range. 
   *     Negative range points are forbidden.
   */
  @Deprecated
    public static MutableRoaringBitmap or(@SuppressWarnings("rawtypes") final Iterator bitmaps, 
          final int rangeStart, final int rangeEnd) {
    return or(bitmaps, (long) rangeStart, (long) rangeEnd);
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
   * @see BufferFastAggregation#or(ImmutableRoaringBitmap...)
   * @see BufferFastAggregation#horizontal_or(ImmutableRoaringBitmap...)
   */
  public static int orCardinality(final ImmutableRoaringBitmap x1,
      final ImmutableRoaringBitmap x2) {
    // we use the fact that the cardinality of the bitmaps is known so that
    // the union is just the total cardinality minus the intersection
    return x1.getCardinality() + x2.getCardinality() - andCardinality(x1, x2);
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
  public static MutableRoaringBitmap xor(@SuppressWarnings("rawtypes") final Iterator bitmaps,
      final long rangeStart, final long rangeEnd) {
    Iterator<ImmutableRoaringBitmap> bitmapsIterator;
    bitmapsIterator = selectRangeWithoutCopy(bitmaps, rangeStart, rangeEnd);
    return BufferFastAggregation.xor(bitmapsIterator);
  }


  /**
   * Computes XOR between input bitmaps in the given range, from rangeStart (inclusive) to rangeEnd
   * (exclusive)
   *
   * @param bitmaps input bitmaps, these are not modified
   * @param rangeStart inclusive beginning of range
   * @param rangeEnd exclusive ending of range
   * @return new result bitmap
   * @deprecated use the version where longs specify the range. 
   *     Negative values not allowed for rangeStart and rangeEnd
   */
  @Deprecated
    public static MutableRoaringBitmap xor(@SuppressWarnings("rawtypes") final Iterator bitmaps, 
          final int rangeStart, final int rangeEnd) {
    return xor(bitmaps, (long) rangeStart, (long) rangeEnd);
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
   * @see BufferFastAggregation#xor(ImmutableRoaringBitmap...)
   * @see BufferFastAggregation#horizontal_xor(ImmutableRoaringBitmap...)
   */
  public static MutableRoaringBitmap xor(final ImmutableRoaringBitmap x1,
      final ImmutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
    MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();
    main: if (i1.hasContainer() && i2.hasContainer()) {
      while (true) {
        if (i1.key() == i2.key()) {
          final MappeableContainer c = i1.getContainer().xor(i2.getContainer());
          if (c.getCardinality() > 0) {
            answer.getMappeableRoaringArray().append(i1.key(), c);
          }
          i1.advance();
          i2.advance();
          if (!i1.hasContainer() || !i2.hasContainer()) {
            break main;
          }
        } else if (Util.compareUnsigned(i1.key(), i2.key()) < 0) { // i1.key() < i2.key()
          answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
          i1.advance();
          if (!i1.hasContainer()) {
            break main;
          }
        } else { // i1.key() < i2.key()
          answer.getMappeableRoaringArray().appendCopy(i2.key(), i2.getContainer());
          i2.advance();
          if (!i2.hasContainer()) {
            break main;
          }
        }
      }
    }
    if (!i1.hasContainer()) {
      while (i2.hasContainer()) {
        answer.getMappeableRoaringArray().appendCopy(i2.key(), i2.getContainer());
        i2.advance();
      }
    } else if (!i2.hasContainer()) {
      while (i1.hasContainer()) {
        answer.getMappeableRoaringArray().appendCopy(i1.key(), i1.getContainer());
        i1.advance();
      }
    }

    return answer;
  }

  PointableRoaringArray highLowContainer = null;


  protected ImmutableRoaringBitmap() {

  }

  /**
   * Constructs a new ImmutableRoaringBitmap starting at this ByteBuffer's position(). Only
   * meta-data is loaded to RAM. The rest is mapped to the ByteBuffer.
   * 
   * It is not necessary that limit() on the input ByteBuffer indicates the end of the serialized
   * data.
   * 
   * After creating this ImmutableRoaringBitmap, you can advance to the rest of the data (if there
   * is more) by setting b.position(b.position() + bitmap.serializedSizeInBytes());
   * 
   * Note that the input ByteBuffer is effectively copied (with the slice operation) so you should
   * expect the provided ByteBuffer to remain unchanged.
   * 
   * 
   * 
   * @param b data source
   */
  public ImmutableRoaringBitmap(final ByteBuffer b) {
    highLowContainer = new ImmutableRoaringArray(b);
  }

  @Override
  public ImmutableRoaringBitmap clone() {
    try {
      final ImmutableRoaringBitmap x = (ImmutableRoaringBitmap) super.clone();
      x.highLowContainer = highLowContainer.clone();
      return x;
    } catch (final CloneNotSupportedException e) {
      throw new RuntimeException("shouldn't happen with clone", e);
    }
  }

  /**
   * Checks whether the value in included, which is equivalent to checking if the corresponding bit
   * is set (get in BitSet class).
   * 
   * @param x integer value
   * @return whether the integer value is included.
   */
  @Override
  public boolean contains(final int x) {
    final short hb = BufferUtil.highbits(x);
    final MappeableContainer c = highLowContainer.getContainer(hb);
    return c != null && c.contains(BufferUtil.lowbits(x));
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ImmutableRoaringBitmap) {
      if (this.highLowContainer.size() != ((ImmutableRoaringBitmap) o).highLowContainer.size()) {
        return false;
      }
      MappeableContainerPointer mp1 = this.highLowContainer.getContainerPointer();
      MappeableContainerPointer mp2 =
          ((ImmutableRoaringBitmap) o).highLowContainer.getContainerPointer();
      while (mp1.hasContainer()) {
        if (mp1.key() != mp2.key()) {
          return false;
        }
        if (mp1.getCardinality() != mp2.getCardinality()) {
          return false;
        }
        if (!mp1.getContainer().equals(mp2.getContainer())) {
          return false;
        }
        mp1.advance();
        mp2.advance();
      }
      return true;
    }
    return false;
  }


  /**
   * Returns the number of distinct integers added to the bitmap (e.g., number of bits set).
   * 
   * @return the cardinality
   */
  @Override
  public long getLongCardinality() {
    long size = 0;
    for (int i = 0; i < this.highLowContainer.size(); ++i) {
      size += this.highLowContainer.getCardinality(i);
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
      highLowContainer.getContainerAtIndex(i).forEach(highLowContainer.getKeyAtIndex(i), ic);
    }
  }



  /**
   * Return a low-level container pointer that can be used to access the underlying data structure.
   * 
   * @return container pointer
   */
  public MappeableContainerPointer getContainerPointer() {
    return this.highLowContainer.getContainerPointer();
  }

  /**
   * For better performance, consider the Use the {@link #forEach forEach} method.
   * 
   * @return a custom iterator over set bits, the bits are traversed in ascending sorted order
   */
  @Override
  public PeekableIntIterator getIntIterator() {
    return new ImmutableRoaringIntIterator();
  }


  /**
   * @return a custom iterator over set bits, the bits are traversed in descending sorted order
   */
  @Override
  public IntIterator getReverseIntIterator() {
    return new ImmutableRoaringReverseIntIterator();
  }

  /**
   * Estimate of the memory usage of this data structure. This can be expected to be within 1% of
   * the true memory usage. If exact measures are needed, we recommend using dedicated libraries
   * such as SizeOf.
   * 
   * When the bitmap is constructed from a ByteBuffer from a memory-mapped file, this estimate is
   * invalid: we can expect the actual memory usage to be significantly (e.g., 10x) less.
   * 
   * @return estimated memory usage.
   */
  @Override
  public long getLongSizeInBytes() {
    long size = 4;
    for (int i = 0; i < this.highLowContainer.size(); ++i) {
      if (this.highLowContainer.getContainerAtIndex(i) instanceof MappeableRunContainer) {
        MappeableRunContainer thisRunContainer =
            (MappeableRunContainer) this.highLowContainer.getContainerAtIndex(i);
        size += 4 + BufferUtil.getSizeInBytesFromCardinalityEtc(0, thisRunContainer.nbrruns, true);
      } else {
        size += 4 + BufferUtil
            .getSizeInBytesFromCardinalityEtc(this.highLowContainer.getCardinality(i), 0, false);
      }
    }
    return size;
  }

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
    return this.highLowContainer.hasRunCompression();
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
      int hs = 0;

      ShortIterator iter;

      short pos = 0;

      int x;

      @Override
      public boolean hasNext() {
        return pos < ImmutableRoaringBitmap.this.highLowContainer.size();
      }

      public Iterator<Integer> init() {
        if (pos < ImmutableRoaringBitmap.this.highLowContainer.size()) {
          iter = ImmutableRoaringBitmap.this.highLowContainer.getContainerAtIndex(pos)
              .getShortIterator();
          hs = BufferUtil
              .toIntUnsigned(ImmutableRoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
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
   * Create a new Roaring bitmap containing at most maxcardinality integers.
   * 
   * @param maxcardinality maximal cardinality
   * @return a new bitmap with cardinality no more than maxcardinality
   */
  @Override
  public MutableRoaringBitmap limit(int maxcardinality) {
    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    int currentcardinality = 0;
    for (int i = 0; (currentcardinality < maxcardinality)
        && (i < this.highLowContainer.size()); i++) {
      MappeableContainer c = this.highLowContainer.getContainerAtIndex(i);
      if (c.getCardinality() + currentcardinality <= maxcardinality) {
        ((MutableRoaringArray) answer.highLowContainer)
            .append(this.highLowContainer.getKeyAtIndex(i), c.clone());
        currentcardinality += c.getCardinality();
      } else {
        int leftover = maxcardinality - currentcardinality;
        MappeableContainer limited = c.limit(leftover);
        ((MutableRoaringArray) answer.highLowContainer)
            .append(this.highLowContainer.getKeyAtIndex(i), limited);
        break;
      }
    }
    return answer;
  }

  /**
   * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be
   * GetCardinality()).
   * 
   * @param x upper limit
   *
   * @return the rank
   */
  @Override
  public long rankLong(int x) {
    long size = 0;
    short xhigh = BufferUtil.highbits(x);
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      short key = this.highLowContainer.getKeyAtIndex(i);
      if (Util.compareUnsigned(key, xhigh) < 0) {
        size += this.highLowContainer.getCardinality(i);
      } else {
        return size + this.highLowContainer.getContainerAtIndex(i).rank(BufferUtil.lowbits(x));
      }
    }
    return size;
  }

  @Override
  public int rank(int x) {
    return (int) rankLong(x);
  }
  
  /**
   * Return the jth value stored in this bitmap.
   * 
   * @param j index of the value
   *
   * @return the value
   */
  @Override
  public int select(int j) {
    int leftover = j;
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      int thiscard = this.highLowContainer.getCardinality(i);
      if (thiscard > leftover) {
        int keycontrib = this.highLowContainer.getKeyAtIndex(i) << 16;
        MappeableContainer c = this.highLowContainer.getContainerAtIndex(i);
        int lowcontrib = BufferUtil.toIntUnsigned(c.select(leftover));
        return lowcontrib + keycontrib;
      }
      leftover -= thiscard;
    }
    throw new IllegalArgumentException(
        "select " + j + " when the cardinality is " + this.getCardinality());
  }

  /**
   * Serialize this bitmap.
   * 
   * Consider calling {@link MutableRoaringBitmap#runOptimize} before serialization to improve
   * compression if this is a MutableRoaringBitmap instance.
   * 
   * The current bitmap is not modified.
   * 
   * Advanced example: To serialize your bitmap to a ByteBuffer, you can do the following.
   * 
   * <pre>
   * {
   *   &#64;code
   *   // r is your bitmap
   *
   *   // r.runOptimize(); // might improve compression, only if you have a
   *   // MutableRoaringBitmap instance.
   *   // next we create the ByteBuffer where the data will be stored
   *   ByteBuffer outbb = ByteBuffer.allocate(r.serializedSizeInBytes());
   *   // then we can serialize on a custom OutputStream
   *   mrb.serialize(new DataOutputStream(new OutputStream() {
   *     ByteBuffer mBB;
   * 
   *     OutputStream init(ByteBuffer mbb) {
   *       mBB = mbb;
   *       return this;
   *     }
   * 
   *     public void close() {}
   * 
   *     public void flush() {}
   * 
   *     public void write(int b) {
   *       mBB.put((byte) b);
   *     }
   * 
   *     public void write(byte[] b) {
   *       mBB.put(b);
   *     }
   * 
   *     public void write(byte[] b, int off, int l) {
   *       mBB.put(b, off, l);
   *     }
   *   }.init(outbb)));
   *   // outbuff will now contain a serialized version of your bitmap
   * }
   * </pre>
   * 
   * Note: Java's data structures are in big endian format. Roaring serializes to a little endian
   * format, so the bytes are flipped by the library during serialization to ensure that what is
   * stored is in little endian---despite Java's big endianness. You can defeat this process by
   * reflipping the bytes again in a custom DataOutput which could lead to serialized Roaring
   * objects with an incorrect byte order.
   *
   * 
   * @param out the DataOutput stream
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Override
  public void serialize(DataOutput out) throws IOException {
    this.highLowContainer.serialize(out);
  }

  /**
   * Report the number of bytes required for serialization. This count will match the bytes written
   * when calling the serialize method.
   * 
   * @return the size in bytes
   */
  @Override
  public int serializedSizeInBytes() {
    return this.highLowContainer.serializedSizeInBytes();
  }


  /**
   * Return the set values as an array if the cardinality is less 
   * than 2147483648. The integer values are in sorted order.
   * 
   * @return array representing the set values.
   */
  @Override
  public int[] toArray() {
    final int[] array = new int[(int)this.getCardinality()];
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
   * Copies the content of this bitmap to a bitmap that can be modified.
   * 
   * @return a mutable bitmap.
   */
  public MutableRoaringBitmap toMutableRoaringBitmap() {
    MutableRoaringBitmap c = new MutableRoaringBitmap();
    MappeableContainerPointer mcp = highLowContainer.getContainerPointer();
    while (mcp.hasContainer()) {
      c.getMappeableRoaringArray().appendCopy(mcp.key(), mcp.getContainer());
      mcp.advance();
    }
    return c;
  }


  /**
   * Copies this bitmap to a mutable RoaringBitmap.
   * 
   * @return a copy of this bitmap as a RoaringBitmap.
   */
  public RoaringBitmap toRoaringBitmap() {
    return new RoaringBitmap(this);
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
      answer.append(i.next());
    }
    while (i.hasNext()) {
      answer.append(",");
      // to avoid using too much memory, we limit the size
      if(answer.length() > 0x80000) {
        answer.append("...");
        break;
      }
      answer.append(i.next());
    }
    answer.append("}");
    return answer.toString();
  }

}
