/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import static org.roaringbitmap.Util.toUnsignedLong;
import static org.roaringbitmap.buffer.BufferUtil.highbits;
import static org.roaringbitmap.buffer.BufferUtil.lowbits;
import static org.roaringbitmap.buffer.BufferUtil.lowbitsAsInteger;
import static org.roaringbitmap.buffer.MutableRoaringBitmap.rangeSanityCheck;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.CharIterator;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableCharIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.Util;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * ImmutableRoaringBitmap provides a compressed immutable (cannot be modified) bitmap. It is meant
 * to be used with org.roaringbitmap.buffer.MutableRoaringBitmap, a derived class that adds methods
 * to modify the bitmap. Because the class ImmutableRoaringBitmap is not final and
 * because there exists one derived class (org.roaringbitmap.buffer.MutableRoaringBitmap), then
 * it is possible for the programmer to modify some ImmutableRoaringBitmap instances,
 * but this invariably involves casting to other classes: if your code is written in terms
 * of ImmutableRoaringBitmap instances, then your objects
 * will be truly immutable, and thus easy to reason about.
 *
 * Pure (non-derived) instances of ImmutableRoaringBitmap have their data backed by a ByteBuffer.
 * This has the benefit that they may be constructed from a ByteBuffer (useful for memory mapping).
 *
 * Objects of this class may reside almost entirely in memory-map files. That is the primary reason
 * for them to be considered immutable, since no reallocation is possible when using
 * memory-mapped files.
 *
 * From a language design point of view, instances of this class are immutable only when used as per
 * the interface of the ImmutableRoaringBitmap class. Given that the class is not final,
 * it is possible to modify instances, through other interfaces. Thus we do not take the term
 * "immutable" in a purist manner,
 * but rather in a practical one.
 *
 * One of our motivations for this design where MutableRoaringBitmap instances can be casted
 * down to ImmutableRoaringBitmap instances is that bitmaps are often large,
 * or used in a context where memory allocations are to be avoided, so we avoid forcing copies.
 * Copies could be expected if one needs to mix and match ImmutableRoaringBitmap and
 * MutableRoaringBitmap instances.
 *
 * <pre>
 * {@code
 *       import org.roaringbitmap.buffer.*;
 *
 *       //...
 *
 *       MutableRoaringBitmap rr1 = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
 *       MutableRoaringBitmap rr2 = MutableRoaringBitmap.bitmapOf(2, 3, 1010);
 *       ByteArrayOutputStream bos = new ByteArrayOutputStream();
 *       DataOutputStream dos = new DataOutputStream(bos);
 *       // could call "rr1.runOptimize()" and "rr2.runOptimize" if
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
 * When deserializing from untrusted source, we recommend calling 'validate()'
 * after deserialization to ensure that the result is a valid bitmap. Furthermore,
 * we recommend using hashing to ensure that the bitmap has not been tampered with.
 *
 *
 * @see MutableRoaringBitmap
 */
public class ImmutableRoaringBitmap
    implements Iterable<Integer>, Cloneable, ImmutableBitmapDataProvider {

  private class ImmutableRoaringIntIterator implements PeekableIntIterator {
    private boolean wrap;
    private MappeableContainerPointer cp;
    private int iterations = 0;

    private int hs = 0;

    private PeekableCharIterator iter;

    private boolean ok;

    public ImmutableRoaringIntIterator() {
      char index = findStartingContainerIndex();
      wrap = index != 0;
      cp = ImmutableRoaringBitmap.this.highLowContainer.getContainerPointer(index);
      nextContainer();
    }

    char findStartingContainerIndex() {
      return 0;
    }

    @Override
    public PeekableIntIterator clone() {
      try {
        ImmutableRoaringIntIterator x = (ImmutableRoaringIntIterator) super.clone();
        if (this.iter != null) {
          x.iter = this.iter.clone();
        }
        if (this.cp != null) {
          x.cp = this.cp.clone();
        }
        x.wrap = this.wrap;
        x.iterations = this.iterations;
        return x;
      } catch (CloneNotSupportedException e) {
        return null; // will not happen
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
      final int containerSize = ImmutableRoaringBitmap.this.highLowContainer.size();
      if (wrap || iterations < containerSize) {
        ok = cp.hasContainer();
        if (!ok && wrap && iterations < containerSize) {
          cp = ImmutableRoaringBitmap.this.highLowContainer.getContainerPointer();
          wrap = false;
          ok = cp.hasContainer();
        }
        if (ok) {
          iter = cp.getContainer().getCharIterator();
          hs = (cp.key()) << 16;
          ++iterations;
        }
      } else {
        ok = false;
      }
    }

    @Override
    public void advanceIfNeeded(int minval) {
      while (hasNext() && shouldAdvanceContainer(hs, minval)) {
        cp.advance();
        nextContainer();
      }
      if (ok && ((hs >>> 16) == (minval >>> 16))) {
        iter.advanceIfNeeded(lowbits(minval));
        if (!iter.hasNext()) {
          cp.advance();
          nextContainer();
        }
      }
    }

    boolean shouldAdvanceContainer(final int hs, final int minval) {
      return (hs >>> 16) < (minval >>> 16);
    }

    @Override
    public int peekNext() {
      return (iter.peekNext()) | hs;
    }
  }

  private class ImmutableRoaringSignedIntIterator extends ImmutableRoaringIntIterator {

    @Override
    char findStartingContainerIndex() {
      // skip to starting at negative signed integers
      char index =
          (char) ImmutableRoaringBitmap.this.highLowContainer.advanceUntil((char) (1 << 15), -1);
      if (index == ImmutableRoaringBitmap.this.highLowContainer.size()) {
        index = 0;
      }
      return index;
    }

    @Override
    boolean shouldAdvanceContainer(final int hs, final int minval) {
      return (hs >> 16) < (minval >> 16);
    }
  }

  private final class ImmutableRoaringReverseIntIterator implements IntIterator {
    private MappeableContainerPointer cp =
        ImmutableRoaringBitmap.this.highLowContainer.getContainerPointer(
            ImmutableRoaringBitmap.this.highLowContainer.size() - 1);

    private int hs = 0;

    private CharIterator iter;

    private boolean ok;

    public ImmutableRoaringReverseIntIterator() {
      nextContainer();
    }

    @Override
    public IntIterator clone() {
      try {
        ImmutableRoaringReverseIntIterator x = (ImmutableRoaringReverseIntIterator) super.clone();
        if (this.iter != null) {
          x.iter = this.iter.clone();
        }
        if (this.cp != null) {
          x.cp = this.cp.clone();
        }
        return x;
      } catch (CloneNotSupportedException e) {
        return null; // will not happen
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
        iter = cp.getContainer().getReverseCharIterator();
        hs = (cp.key()) << 16;
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
  public static MutableRoaringBitmap and(
      final Iterator<? extends ImmutableRoaringBitmap> bitmaps,
      final long rangeStart,
      final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
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
  public static MutableRoaringBitmap and(
      final Iterator<? extends ImmutableRoaringBitmap> bitmaps,
      final int rangeStart,
      final int rangeEnd) {
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
  public static MutableRoaringBitmap and(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      if (s1 == s2) {
        final MappeableContainer c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        final MappeableContainer c = c1.and(c2);
        if (!c.isEmpty()) {
          answer.getMappeableRoaringArray().append(s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) {
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
  public static int andCardinality(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
    int answer = 0;
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      if (s1 == s2) {
        final MappeableContainer c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        answer += c1.andCardinality(c2);
        ++pos1;
        ++pos2;
      } else if (s1 < s2) {
        pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
      } else { // s1 > s2
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    return answer;
  }

  /**
   * Validate the content of the bitmap. Useful after deserialization.
   * @return true if the content is valid.
   */
  public Boolean validate() {
    return this.highLowContainer.validate();
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
  public static int xorCardinality(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
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
  public static int andNotCardinality(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
    return x1.getCardinality() - andCardinality(x1, x2);
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
  public static MutableRoaringBitmap andNot(
      final ImmutableRoaringBitmap x1,
      final ImmutableRoaringBitmap x2,
      long rangeStart,
      long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
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
  public static MutableRoaringBitmap andNot(
      final ImmutableRoaringBitmap x1,
      final ImmutableRoaringBitmap x2,
      final int rangeStart,
      final int rangeEnd) {
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
  public static MutableRoaringBitmap andNot(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        final MappeableContainer c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        final MappeableContainer c = c1.andNot(c2);
        if (!c.isEmpty()) {
          answer.getMappeableRoaringArray().append(s1, c);
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) {
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
  public static MutableRoaringBitmap orNot(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2, long rangeEnd) {
    rangeSanityCheck(0, rangeEnd);
    int maxKey = (int) ((rangeEnd - 1) >>> 16);
    int lastRun = (rangeEnd & 0xFFFF) == 0 ? 0x10000 : (int) (rangeEnd & 0xFFFF);
    int size = 0;
    int pos1 = 0, pos2 = 0;
    int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
    int s1 = length1 > 0 ? x1.highLowContainer.getKeyAtIndex(pos1) : maxKey + 1;
    int s2 = length2 > 0 ? x2.highLowContainer.getKeyAtIndex(pos2) : maxKey + 1;
    int remainder = 0;
    for (int i = x1.highLowContainer.size() - 1;
        i >= 0 && x1.highLowContainer.getKeyAtIndex(i) > maxKey;
        --i) {
      ++remainder;
    }
    int correction = 0;
    for (int i = 0; i < x2.highLowContainer.size() - remainder; ++i) {
      correction += x2.highLowContainer.getContainerAtIndex(i).isFull() ? 1 : 0;
      if (x2.highLowContainer.getKeyAtIndex(i) >= maxKey) {
        break;
      }
    }
    // it's almost certain that the bitmap will grow, so make a conservative overestimate,
    // this avoids temporary allocation, and can trim afterwards
    int maxSize =
        Math.min(maxKey + 1 + remainder - correction + x1.highLowContainer.size(), 0x10000);
    if (maxSize == 0) {
      return new MutableRoaringBitmap();
    }
    char[] newKeys = new char[maxSize];
    MappeableContainer[] newValues = new MappeableContainer[maxSize];
    for (int key = 0; key <= maxKey && size < maxSize; ++key) {
      if (key == s1 && key == s2) { // actually need to do an or not
        newValues[size] =
            x1.highLowContainer
                .getContainerAtIndex(pos1)
                .orNot(
                    x2.highLowContainer.getContainerAtIndex(pos2),
                    key == maxKey ? lastRun : 0x10000);
        ++pos1;
        ++pos2;
        s1 = pos1 < length1 ? x1.highLowContainer.getKeyAtIndex(pos1) : maxKey + 1;
        s2 = pos2 < length2 ? x2.highLowContainer.getKeyAtIndex(pos2) : maxKey + 1;
      } else if (key == s1) { // or in a hole
        newValues[size] =
            key == maxKey
                ? x1.highLowContainer
                    .getContainerAtIndex(pos1)
                    .ior(MappeableRunContainer.rangeOfOnes(0, lastRun))
                : MappeableRunContainer.full();
        ++pos1;
        s1 = pos1 < length1 ? x1.highLowContainer.getKeyAtIndex(pos1) : maxKey + 1;
      } else if (key == s2) { // insert the complement
        newValues[size] =
            x2.highLowContainer.getContainerAtIndex(pos2).not(0, key == maxKey ? lastRun : 0x10000);
        ++pos2;
        s2 = pos2 < length2 ? x2.highLowContainer.getKeyAtIndex(pos2) : maxKey + 1;
      } else { // key missing from both
        newValues[size] =
            key == maxKey
                ? MappeableRunContainer.rangeOfOnes(0, lastRun)
                : MappeableRunContainer.full();
      }
      // might have appended an empty container (rare case)
      if (newValues[size].isEmpty()) {
        newValues[size] = null;
      } else {
        newKeys[size++] = (char) key;
      }
    }
    // copy over everything which will remain without being complemented
    if (remainder > 0) {
      for (int i = 0; i < remainder; ++i) {
        int source = x1.highLowContainer.size() - remainder + i;
        int target = size + i;
        newKeys[target] = x1.highLowContainer.getKeyAtIndex(source);
        newValues[target] = x1.highLowContainer.getContainerAtIndex(source).clone();
      }
    }
    return new MutableRoaringBitmap(new MutableRoaringArray(newKeys, newValues, size + remainder));
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
  public static MutableRoaringBitmap flip(
      ImmutableRoaringBitmap bm, final long rangeStart, final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
    if (rangeStart >= rangeEnd) {
      throw new RuntimeException("Invalid range " + rangeStart + " -- " + rangeEnd);
    }

    MutableRoaringBitmap answer = new MutableRoaringBitmap();
    final char hbStart = highbits(rangeStart);
    final char lbStart = lowbits(rangeStart);
    final char hbLast = highbits(rangeEnd - 1);
    final char lbLast = lowbits(rangeEnd - 1);

    // copy the containers before the active area
    answer.getMappeableRoaringArray().appendCopiesUntil(bm.highLowContainer, hbStart);

    final int max = (BufferUtil.maxLowBit());
    for (char hb = hbStart; hb <= hbLast; ++hb) {
      final int containerStart = (hb == hbStart) ? (lbStart) : 0;
      final int containerLast = (hb == hbLast) ? (lbLast) : max;

      final int i = bm.highLowContainer.getIndex(hb);
      final int j = answer.getMappeableRoaringArray().getIndex(hb);
      assert j < 0;

      if (i >= 0) {
        final MappeableContainer c =
            bm.highLowContainer.getContainerAtIndex(i).not(containerStart, containerLast + 1);
        if (!c.isEmpty()) {
          answer.getMappeableRoaringArray().insertNewKeyValueAt(-j - 1, hb, c);
        }

      } else { // *think* the range of ones must never be
        // empty.
        answer
            .getMappeableRoaringArray()
            .insertNewKeyValueAt(
                -j - 1, hb, MappeableContainer.rangeOfOnes(containerStart, containerLast + 1));
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
  public static MutableRoaringBitmap flip(
      ImmutableRoaringBitmap bm, final int rangeStart, final int rangeEnd) {
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
   * @param bitmaps bitmaps iterator
   * @param rangeStart inclusive
   * @param rangeEnd exclusive
   * @return new iterator of bitmaps
   */
  private static Iterator<ImmutableRoaringBitmap> selectRangeWithoutCopy(
      final Iterator<? extends ImmutableRoaringBitmap> bitmaps,
      final long rangeStart,
      final long rangeEnd) {
    Iterator<ImmutableRoaringBitmap> bitmapsIterator;
    bitmapsIterator =
        new Iterator<ImmutableRoaringBitmap>() {
          @Override
          public boolean hasNext() {
            return bitmaps.hasNext();
          }

          @Override
          public ImmutableRoaringBitmap next() {
            ImmutableRoaringBitmap next = bitmaps.next();
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
   * Creates a copy of the bitmap, limited to the values in the specified range,
   * rangeStart (inclusive) and rangeEnd (exclusive).
   *
   * @param rangeStart inclusive
   * @param rangeEnd exclusive
   * @return new bitmap
   */
  public MutableRoaringBitmap selectRange(final long rangeStart, final long rangeEnd) {
    final int hbStart = (BufferUtil.highbits(rangeStart));
    final int lbStart = (BufferUtil.lowbits(rangeStart));
    final int hbLast = (BufferUtil.highbits(rangeEnd - 1));
    final int lbLast = (BufferUtil.lowbits(rangeEnd - 1));
    MutableRoaringBitmap answer = new MutableRoaringBitmap();

    assert (rangeStart >= 0 && rangeEnd >= 0);

    if (rangeEnd <= rangeStart) {
      return answer;
    }

    if (hbStart == hbLast) {
      final int i = this.highLowContainer.getIndex((char) hbStart);
      if (i >= 0) {
        final MappeableContainer c =
            this.highLowContainer
                .getContainerAtIndex(i)
                .remove(0, lbStart)
                .iremove(lbLast + 1, Util.maxLowBitAsInteger() + 1);
        if (!c.isEmpty()) {
          ((MutableRoaringArray) answer.highLowContainer).append((char) hbStart, c);
        }
      }
      return answer;
    }
    int ifirst = this.highLowContainer.getIndex((char) hbStart);
    int ilast = this.highLowContainer.getIndex((char) hbLast);
    if (ifirst >= 0) {
      MappeableContainer c = this.highLowContainer.getContainerAtIndex(ifirst).remove(0, lbStart);
      if (!c.isEmpty()) {
        ((MutableRoaringArray) answer.highLowContainer).append((char) hbStart, c.clone());
      }
    }

    // revised to loop on ints
    for (int hb = hbStart + 1; hb <= hbLast - 1; ++hb) {
      final int i = this.highLowContainer.getIndex((char) hb);
      final int j = answer.highLowContainer.getIndex((char) hb);
      assert j < 0;

      if (i >= 0) {
        final MappeableContainer c = this.highLowContainer.getContainerAtIndex(i);
        ((MutableRoaringArray) answer.highLowContainer)
            .insertNewKeyValueAt(-j - 1, (char) hb, c.clone());
      }
    }

    if (ilast >= 0) {
      MappeableContainer c =
          this.highLowContainer
              .getContainerAtIndex(ilast)
              .remove(lbLast + 1, Util.maxLowBitAsInteger() + 1);
      if (!c.isEmpty()) {
        ((MutableRoaringArray) answer.highLowContainer).append((char) hbLast, c);
      }
    }
    return answer;
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
  private static MutableRoaringBitmap selectRangeWithoutCopy(
      ImmutableRoaringBitmap rb, final long rangeStart, final long rangeEnd) {
    final int hbStart = (highbits(rangeStart));
    final int lbStart = (lowbits(rangeStart));
    final int hbLast = (highbits(rangeEnd - 1));
    final int lbLast = (lowbits(rangeEnd - 1));
    MutableRoaringBitmap answer = new MutableRoaringBitmap();

    if (rangeEnd <= rangeStart) {
      return answer;
    }

    if (hbStart == hbLast) {
      final int i = rb.highLowContainer.getIndex((char) hbStart);
      if (i >= 0) {
        final MappeableContainer c =
            rb.highLowContainer
                .getContainerAtIndex(i)
                .remove(0, lbStart)
                .iremove(lbLast + 1, BufferUtil.maxLowBitAsInteger() + 1);
        if (!c.isEmpty()) {
          ((MutableRoaringArray) answer.highLowContainer).append((char) hbStart, c);
        }
      }
      return answer;
    }
    int ifirst = rb.highLowContainer.getIndex((char) hbStart);
    int ilast = rb.highLowContainer.getIndex((char) hbLast);
    if (ifirst >= 0) {
      final MappeableContainer c =
          rb.highLowContainer.getContainerAtIndex(ifirst).remove(0, lbStart);
      if (!c.isEmpty()) {
        ((MutableRoaringArray) answer.highLowContainer).append((char) hbStart, c);
      }
    }

    for (int hb = hbStart + 1; hb <= hbLast - 1; ++hb) {
      final int i = rb.highLowContainer.getIndex((char) hb);
      final int j = answer.getMappeableRoaringArray().getIndex((char) hb);
      assert j < 0;

      if (i >= 0) {
        final MappeableContainer c = rb.highLowContainer.getContainerAtIndex(i);
        answer.getMappeableRoaringArray().insertNewKeyValueAt(-j - 1, (char) hb, c);
      }
    }

    if (ilast >= 0) {
      final MappeableContainer c =
          rb.highLowContainer
              .getContainerAtIndex(ilast)
              .remove(lbLast + 1, BufferUtil.maxLowBitAsInteger() + 1);
      if (!c.isEmpty()) {
        ((MutableRoaringArray) answer.highLowContainer).append((char) hbLast, c);
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
  public static boolean intersects(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
    int pos1 = 0, pos2 = 0;
    final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

    while (pos1 < length1 && pos2 < length2) {
      final char s1 = x1.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = x2.highLowContainer.getKeyAtIndex(pos2);

      if (s1 == s2) {
        final MappeableContainer c1 = x1.highLowContainer.getContainerAtIndex(pos1);
        final MappeableContainer c2 = x2.highLowContainer.getContainerAtIndex(pos2);
        if (c1.intersects(c2)) {
          return true;
        }
        ++pos1;
        ++pos2;
      } else if (s1 < s2) {
        pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
      } else { // s1 > s2
        pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
      }
    }
    return false;
  }

  // important: inputs should not be reused
  protected static MutableRoaringBitmap lazyor(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
    MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();
    main:
    if (i1.hasContainer() && i2.hasContainer()) {
      while (true) {
        if (i1.key() == i2.key()) {
          answer
              .getMappeableRoaringArray()
              .append(i1.key(), i1.getContainer().lazyOR(i2.getContainer()));
          i1.advance();
          i2.advance();
          if (!i1.hasContainer() || !i2.hasContainer()) {
            break main;
          }
        } else if (i1.key() < i2.key()) {
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
  public static MutableRoaringBitmap or(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
    MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();
    main:
    if (i1.hasContainer() && i2.hasContainer()) {
      while (true) {
        if (i1.key() == i2.key()) {
          answer
              .getMappeableRoaringArray()
              .append(i1.key(), i1.getContainer().or(i2.getContainer()));
          i1.advance();
          i2.advance();
          if (!i1.hasContainer() || !i2.hasContainer()) {
            break main;
          }
        } else if (i1.key() < i2.key()) {
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
  public static MutableRoaringBitmap or(Iterator<? extends ImmutableRoaringBitmap> bitmaps) {
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
  public static MutableRoaringBitmap or(
      final Iterator<? extends ImmutableRoaringBitmap> bitmaps,
      final long rangeStart,
      final long rangeEnd) {
    rangeSanityCheck(rangeStart, rangeEnd);
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
  public static MutableRoaringBitmap or(
      final Iterator<? extends ImmutableRoaringBitmap> bitmaps,
      final int rangeStart,
      final int rangeEnd) {
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
  public static int orCardinality(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
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
  public static MutableRoaringBitmap xor(
      final Iterator<? extends ImmutableRoaringBitmap> bitmaps,
      final long rangeStart,
      final long rangeEnd) {
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
  public static MutableRoaringBitmap xor(
      final Iterator<? extends ImmutableRoaringBitmap> bitmaps,
      final int rangeStart,
      final int rangeEnd) {
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
  public static MutableRoaringBitmap xor(
      final ImmutableRoaringBitmap x1, final ImmutableRoaringBitmap x2) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    MappeableContainerPointer i1 = x1.highLowContainer.getContainerPointer();
    MappeableContainerPointer i2 = x2.highLowContainer.getContainerPointer();
    main:
    if (i1.hasContainer() && i2.hasContainer()) {
      while (true) {
        if (i1.key() == i2.key()) {
          final MappeableContainer c = i1.getContainer().xor(i2.getContainer());
          if (!c.isEmpty()) {
            answer.getMappeableRoaringArray().append(i1.key(), c);
          }
          i1.advance();
          i2.advance();
          if (!i1.hasContainer() || !i2.hasContainer()) {
            break main;
          }
        } else if (i1.key() < i2.key()) {
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

  protected ImmutableRoaringBitmap() {}

  /**
   * Constructs a new ImmutableRoaringBitmap starting at this ByteBuffer's position(). Only
   * meta-data is loaded to RAM. The rest is mapped to the ByteBuffer. The byte stream should
   * abide by the format specification https://github.com/RoaringBitmap/RoaringFormatSpec
   *
   * It is not necessary that limit() on the input ByteBuffer indicates the end of the serialized
   * data.
   *
   * When deserializing from untrusted source, we recommend calling 'validate()'
   * after deserialization to ensure that the result is a valid bitmap. Furthermore,
   * we recommend using hashing to ensure that the bitmap has not been tampered with.
   *
   * After creating this ImmutableRoaringBitmap, you can advance to the rest of the data (if there
   * is more) by setting b.position(b.position() + bitmap.serializedSizeInBytes());
   *
   * Note that the input ByteBuffer is effectively copied (with the slice operation) so you should
   * expect the provided ByteBuffer position/mark/limit/order to remain unchanged.
   *
   * This constructor may throw IndexOutOfBoundsException if the input is invalid/corrupted.
   * This constructor throws an InvalidRoaringFormat if the provided input
   * does not have a valid cookie or suffers from similar problems.
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
    final char hb = highbits(x);
    int index = highLowContainer.getContainerIndex(hb);
    return index >= 0 && highLowContainer.containsForContainerAtIndex(index, lowbits(x));
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
    char firstKey = highbits(minimum);
    char lastKey = highbits(supremum);
    int span = (lastKey) - (firstKey);
    int len = highLowContainer.size();
    if (len < span) {
      return false;
    }
    int begin = highLowContainer.getIndex(firstKey);
    int end = highLowContainer.getIndex(lastKey);
    end = end < 0 ? -end - 1 : end;
    if (begin < 0 || end - begin != span) {
      return false;
    }

    int min = (char) minimum;
    int sup = (char) supremum;
    if (firstKey == lastKey) {
      return highLowContainer
          .getContainerAtIndex(begin)
          .contains(min, (supremum & 0xFFFF) == 0 ? 0x10000 : sup);
    }
    if (!highLowContainer.getContainerAtIndex(begin).contains(min, 1 << 16)) {
      return false;
    }
    if (sup != 0 && end < len && !highLowContainer.getContainerAtIndex(end).contains(0, sup)) {
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
   * Checks whether the parameter is a subset of this RoaringBitmap or not
   * @param subset the potential subset
   * @return true if the parameter is a subset of this RoaringBitmap
   */
  public boolean contains(ImmutableRoaringBitmap subset) {
    final int length1 = this.highLowContainer.size();
    final int length2 = subset.highLowContainer.size();
    int pos1 = 0, pos2 = 0;
    while (pos1 < length1 && pos2 < length2) {
      final char s1 = this.highLowContainer.getKeyAtIndex(pos1);
      final char s2 = subset.highLowContainer.getKeyAtIndex(pos2);
      if (s1 == s2) {
        MappeableContainer c1 = this.highLowContainer.getContainerAtIndex(pos1);
        MappeableContainer c2 = subset.highLowContainer.getContainerAtIndex(pos2);
        if (!c1.contains(c2)) {
          return false;
        }
        ++pos1;
        ++pos2;
      } else if ((s1) - (s2) > 0) {
        return false;
      } else {
        pos1 = subset.highLowContainer.advanceUntil(s2, pos1);
      }
    }
    return pos2 == length2;
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
   * Returns true if the other bitmap has no more than tolerance bits
   * differing from this bitmap. The other may be transformed into a bitmap equal
   * to this bitmap in no more than tolerance bit flips if this method returns true.
   *
   * @param other the bitmap to compare to
   * @param tolerance the maximum number of bits that may differ
   * @return true if the number of differing bits is smaller than tolerance
   */
  public boolean isHammingSimilar(ImmutableRoaringBitmap other, int tolerance) {
    final int size1 = highLowContainer.size();
    final int size2 = other.highLowContainer.size();
    int pos1 = 0;
    int pos2 = 0;
    int budget = tolerance;
    while (budget >= 0 && pos1 < size1 && pos2 < size2) {
      final char key1 = highLowContainer.getKeyAtIndex(pos1);
      final char key2 = other.highLowContainer.getKeyAtIndex(pos2);
      MappeableContainer left = highLowContainer.getContainerAtIndex(pos1);
      MappeableContainer right = other.highLowContainer.getContainerAtIndex(pos2);
      if (key1 == key2) {
        budget -= left.xorCardinality(right);
        ++pos1;
        ++pos2;
      } else if (key1 < key2) {
        budget -= left.getCardinality();
        ++pos1;
      } else {
        budget -= right.getCardinality();
        ++pos2;
      }
    }
    while (budget >= 0 && pos1 < size1) {
      MappeableContainer container = highLowContainer.getContainerAtIndex(pos1++);
      budget -= container.getCardinality();
    }
    while (budget >= 0 && pos2 < size2) {
      MappeableContainer container = other.highLowContainer.getContainerAtIndex(pos2++);
      budget -= container.getCardinality();
    }
    return budget >= 0;
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
    int minKey = (int) (minimum >>> 16);
    int supKey = (int) (supremum >>> 16);
    int length = highLowContainer.size();
    // seek to start
    int pos = 0;
    while (pos < length && minKey > (highLowContainer.getKeyAtIndex(pos))) {
      ++pos;
    }
    // it is possible for pos == length to be true
    if (pos == length) {
      return false;
    }
    // we have that pos < length.
    int offset = (minKey == highLowContainer.getKeyAtIndex(pos)) ? lowbitsAsInteger(minimum) : 0;
    int limit = lowbitsAsInteger(supremum);
    if (supKey == (highLowContainer.getKeyAtIndex(pos))) {
      if (supKey > minKey) {
        offset = 0;
      }
      return highLowContainer.getContainerAtIndex(pos).intersects(offset, limit);
    }
    while (pos < length && supKey > (highLowContainer.getKeyAtIndex(pos))) {
      MappeableContainer container = highLowContainer.getContainerAtIndex(pos);
      if (container.intersects(offset, 1 << 16)) {
        return true;
      }
      offset = 0;
      ++pos;
    }
    return pos < length
        && supKey == highLowContainer.getKeyAtIndex(pos)
        && highLowContainer.getContainerAtIndex(pos).intersects(offset, limit);
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

  /**
   * Returns true if the bitmap's cardinality exceeds the threshold.
   * @param threshold threshold
   * @return true if the cardinality exceeds the threshold.
   */
  public boolean cardinalityExceeds(long threshold) {
    long size = 0;
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      size += this.highLowContainer.getContainerAtIndex(i).getCardinality();
      if (size > threshold) {
        return true;
      }
    }
    return false;
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
   * @return a custom iterator over set bits, the bits are traversed in unsigned integer ascending
   *     sorted order
   */
  @Override
  public PeekableIntIterator getIntIterator() {
    return new ImmutableRoaringIntIterator();
  }

  /**
   * @return a custom iterator over set bits, the bits are traversed in signed integer ascending
   *     sorted order
   */
  @Override
  public PeekableIntIterator getSignedIntIterator() {
    return new ImmutableRoaringSignedIntIterator();
  }

  /**
   * @return a custom iterator over set bits, the bits are traversed in descending sorted order
   */
  @Override
  public IntIterator getReverseIntIterator() {
    return new ImmutableRoaringReverseIntIterator();
  }

  @Override
  public BatchIterator getBatchIterator() {
    return new RoaringBatchIterator(null == highLowContainer ? null : getContainerPointer());
  }

  /**
   * Estimate of the memory usage of this data structure. This can be expected to be within 1% of
   * the true memory usage in common usage scenarios.
   * If exact measures are needed, we recommend using dedicated libraries
   * such as ehcache-sizeofengine.
   *
   * When the bitmap is constructed from a ByteBuffer from a memory-mapped file, this estimate is
   * invalid: we can expect the actual memory usage to be significantly (e.g., 10x) less.
   *
   * In adversarial cases, this estimate may be 10x the actual memory usage. For example, if
   * you insert a single random value in a bitmap, then over a 100 bytes may be used by the JVM
   * whereas this function may return an estimate of 32 bytes.
   *
   * The same will be true in the "sparse" scenario where you have a small set of random-looking
   * integers spanning a wide range of values.
   *
   * These are considered adversarial cases because, as a general rule, if your
   * data looks like a set
   * of random integers, Roaring bitmaps are probably not the right data structure.
   *
   * Note that you can serialize your Roaring Bitmaps to disk and then construct
   * ImmutableRoaringBitmap
   * instances from a ByteBuffer. In such cases, the Java heap usage will be significantly less than
   * what is reported.
   *
   * If your main goal is to compress arrays of integers, there are other libraries that are maybe
   * more appropriate such as JavaFastPFOR.
   *
   * Note, however, that in general, random integers (as produced by random number
   * generators or hash
   * functions) are not compressible.
   * Trying to compress random data is an adversarial use case.
   *
   * @see <a href="https://github.com/lemire/JavaFastPFOR">JavaFastPFOR</a>
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
        size +=
            4
                + BufferUtil.getSizeInBytesFromCardinalityEtc(
                    this.highLowContainer.getCardinality(i), 0, false);
      }
    }
    return size;
  }

  /**
   * Estimate of the memory usage of this data structure. This can be expected to be
   * within 1% of  the true memory usage in common usage scenarios.
   * If exact measures are needed, we recommend using dedicated libraries
   * such as ehcache-sizeofengine.
   *
   * When the bitmap is constructed from a ByteBuffer from a memory-mapped
   * file, this
   * estimate is invalid: we can expect the actual memory usage to be significantly
   * (e.g., 10x) less.
   *
   * In adversarial cases, this estimate may be 10x the actual memory usage. For example, if
   * you insert a single random value in a bitmap, then over a 100 bytes may be used by the JVM
   * whereas this function may return an estimate of 32 bytes.
   *
   * The same will be true in the "sparse" scenario where you have a small set of random-looking
   * integers spanning a wide range of values.
   *
   * These are considered adversarial cases because, as a general rule, if your data
   * looks like a set of random integers, Roaring bitmaps are probably not the right data structure.
   *
   * Note that you can serialize your Roaring Bitmaps to disk and then construct
   * ImmutableRoaringBitmap instances from a ByteBuffer. In such cases, the Java heap usage
   * will be significantly less than what is reported.
   *
   * If your main goal is to compress arrays of integers, there are other libraries that are
   * maybe more appropriate such as JavaFastPFOR.
   *
   * Note, however, that in general, random integers (as produced by random number
   * generators or hash
   * functions) are not compressible. Trying to compress random data is an
   * adversarial use case.
   *
   * @see <a href="https://github.com/lemire/JavaFastPFOR">JavaFastPFOR</a>
   *
   * @return estimated memory usage.
   */
  @Override
  public int getSizeInBytes() {
    return (int) getLongSizeInBytes();
  }

  /**
   * Compute the hashCode() of this bitmap.
   *
   * For performance reasons, this method deliberately violates the
   * Java contract regarding hashCode/equals in the following manner:
   * If the two bitmaps are equal *and* they have the same
   * hasRunCompression() result, then they have the same hashCode().
   *
   * Thus, for the Java contract to be satisfied, you should either
   * call runOptimize() on all your bitmaps, or on none of your bitmaps.
   *
   * @return the hash code
   */
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

      CharIterator iter;

      int pos = 0;

      int x;

      @Override
      public boolean hasNext() {
        return pos < ImmutableRoaringBitmap.this.highLowContainer.size();
      }

      public Iterator<Integer> init() {
        if (pos < ImmutableRoaringBitmap.this.highLowContainer.size()) {
          iter =
              ImmutableRoaringBitmap.this
                  .highLowContainer
                  .getContainerAtIndex(pos)
                  .getCharIterator();
          hs = (ImmutableRoaringBitmap.this.highLowContainer.getKeyAtIndex(pos)) << 16;
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
    for (int i = 0;
        (currentcardinality < maxcardinality) && (i < this.highLowContainer.size());
        i++) {
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
   * GetCardinality()). If you provide the smallest value as a parameter, this function will
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
    char xhigh = highbits(x);
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      char key = this.highLowContainer.getKeyAtIndex(i);
      if (key < xhigh) {
        size += this.highLowContainer.getCardinality(i);
      } else if (key == xhigh) {
        return size + this.highLowContainer.getContainerAtIndex(i).rank(lowbits(x));
      }
    }
    return size;
  }

  @Override
  public long rangeCardinality(long start, long end) {
    if (Long.compareUnsigned(start, end) >= 0) {
      return 0;
    }
    long size = 0;
    int startIndex = this.highLowContainer.getIndex(highbits(start));
    if (startIndex < 0) {
      startIndex = -startIndex - 1;
    } else {
      int inContainerStart = (lowbits(start));
      if (inContainerStart != 0) {
        size -=
            this.highLowContainer
                .getContainerAtIndex(startIndex)
                .rank((char) (inContainerStart - 1));
      }
    }
    char xhigh = highbits(end - 1);
    for (int i = startIndex; i < this.highLowContainer.size(); i++) {
      char key = this.highLowContainer.getKeyAtIndex(i);
      if (key < xhigh) {
        size += this.highLowContainer.getContainerAtIndex(i).getCardinality();
      } else if (key == xhigh) {
        return size + this.highLowContainer.getContainerAtIndex(i).rank(lowbits((int) (end - 1)));
      }
    }
    return size;
  }

  @Override
  public int rank(int x) {
    return (int) rankLong(x);
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
    long leftover = toUnsignedLong(j);
    for (int i = 0; i < this.highLowContainer.size(); i++) {
      int thiscard = this.highLowContainer.getCardinality(i);
      if (thiscard > leftover) {
        int keycontrib = this.highLowContainer.getKeyAtIndex(i) << 16;
        MappeableContainer c = this.highLowContainer.getContainerAtIndex(i);
        int lowcontrib = (c.select((int) leftover));
        return lowcontrib + keycontrib;
      }
      leftover -= thiscard;
    }
    throw new IllegalArgumentException(
        "You are trying to select the "
            + j
            + "th value when the cardinality is "
            + this.getCardinality()
            + ".");
  }

  @Override
  public int first() {
    return highLowContainer.first();
  }

  @Override
  public int last() {
    return highLowContainer.last();
  }

  @Override
  public int firstSigned() {
    return highLowContainer.firstSigned();
  }

  @Override
  public int lastSigned() {
    return highLowContainer.lastSigned();
  }

  @Override
  public long nextValue(int fromValue) {
    char key = highbits(fromValue);
    int containerIndex = highLowContainer.advanceUntil(key, -1);
    long nextSetBit = -1L;
    while (containerIndex < highLowContainer.size() && nextSetBit == -1L) {
      char containerKey = highLowContainer.getKeyAtIndex(containerIndex);
      MappeableContainer container = highLowContainer.getContainerAtIndex(containerIndex);
      int bit =
          ((containerKey) - (key) > 0
              ? container.first()
              : container.nextValue(lowbits(fromValue)));
      nextSetBit = bit == -1 ? -1L : toUnsignedLong((containerKey << 16) | bit);
      ++containerIndex;
    }
    assert nextSetBit <= 0xFFFFFFFFL;
    assert nextSetBit == -1L || nextSetBit >= toUnsignedLong(fromValue);
    return nextSetBit;
  }

  @Override
  public long previousValue(int fromValue) {
    if (isEmpty()) {
      return -1L;
    }
    char key = highbits(fromValue);
    int containerIndex = highLowContainer.advanceUntil(key, -1);
    if (containerIndex == highLowContainer.size()) {
      return Util.toUnsignedLong(last());
    }
    if (highLowContainer.getKeyAtIndex(containerIndex) > key) {
      // target absent, key of first container after target too high
      --containerIndex;
    }
    long prevSetBit = -1L;
    while (containerIndex != -1 && prevSetBit == -1L) {
      char containerKey = highLowContainer.getKeyAtIndex(containerIndex);
      MappeableContainer container = highLowContainer.getContainerAtIndex(containerIndex);
      int bit =
          (containerKey < key ? container.last() : container.previousValue(lowbits(fromValue)));
      prevSetBit = bit == -1 ? -1L : toUnsignedLong((containerKey << 16) | bit);
      --containerIndex;
    }
    assert prevSetBit <= 0xFFFFFFFFL;
    assert prevSetBit <= toUnsignedLong(fromValue);
    return prevSetBit;
  }

  @Override
  public long nextAbsentValue(int fromValue) {
    long nextAbsentBit = computeNextAbsentValue(fromValue);
    if (nextAbsentBit == 0x100000000L) {
      return -1L;
    }
    return nextAbsentBit;
  }

  private long computeNextAbsentValue(int fromValue) {
    char key = highbits(fromValue);
    int containerIndex = highLowContainer.advanceUntil(key, -1);

    int size = highLowContainer.size();
    if (containerIndex == size) {
      return Util.toUnsignedLong(fromValue);
    }
    char containerKey = highLowContainer.getKeyAtIndex(containerIndex);
    if (fromValue < containerKey << 16) {
      return Util.toUnsignedLong(fromValue);
    }
    MappeableContainer container = highLowContainer.getContainerAtIndex(containerIndex);
    int bit = container.nextAbsentValue(lowbits(fromValue));
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
    char key = highbits(fromValue);
    int containerIndex = highLowContainer.advanceUntil(key, -1);

    if (containerIndex == highLowContainer.size()) {
      return Util.toUnsignedLong(fromValue);
    }
    char containerKey = highLowContainer.getKeyAtIndex(containerIndex);
    if (fromValue < containerKey << 16) {
      return Util.toUnsignedLong(fromValue);
    }
    MappeableContainer container = highLowContainer.getContainerAtIndex(containerIndex);
    int bit = container.previousAbsentValue(lowbits(fromValue));

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
   * Serialize this bitmap.
   *
   *  See format specification at https://github.com/RoaringBitmap/RoaringFormatSpec
   *
   * Consider calling {@link MutableRoaringBitmap#runOptimize} before serialization to improve
   * compression if this is a MutableRoaringBitmap instance.
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
    this.highLowContainer.serialize(buffer);
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
    final int[] array = new int[this.getCardinality()];
    int pos = 0, pos2 = 0;
    while (pos < this.highLowContainer.size()) {
      final int hs = (this.highLowContainer.getKeyAtIndex(pos)) << 16;
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
    final StringBuilder answer = new StringBuilder("{}".length() + "-123456789,".length() * 256);
    final IntIterator i = this.getIntIterator();
    answer.append('{');
    if (i.hasNext()) {
      answer.append(i.next() & 0xFFFFFFFFL);
    }
    while (i.hasNext()) {
      answer.append(',');
      // to avoid using too much memory, we limit the size
      if (answer.length() > 0x80000) {
        answer.append('.').append('.').append('.');
        break;
      }
      answer.append(i.next() & 0xFFFFFFFFL);
    }
    answer.append('}');
    return answer.toString();
  }

  /**
   * Returns the number of containers in the bitmap.
   *
   * @return the number of containers
   */
  @Override
  public int getContainerCount() {
    return highLowContainer.size();
  }
}
