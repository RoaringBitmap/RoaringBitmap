/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.PeekableCharIterator;
import org.roaringbitmap.PeekableIntIterator;

/**
 * Fast iterator minimizing the stress on the garbage collector. You can create one reusable
 * instance of this class and then {@link #wrap(ImmutableRoaringBitmap)}
 *
 * This iterator enumerates the stored values in reverse (starting from the end).
 *
 * @author Borislav Ivanov
 **/
public class BufferReverseIntIteratorFlyweight implements PeekableIntIterator {

  private int hs;

  private PeekableCharIterator iter;

  private ReverseMappeableArrayContainerCharIterator arrIter =
      new ReverseMappeableArrayContainerCharIterator();

  private ReverseMappeableBitmapContainerCharIterator bitmapIter =
      new ReverseMappeableBitmapContainerCharIterator();

  private ReverseMappeableRunContainerCharIterator runIter =
      new ReverseMappeableRunContainerCharIterator();

  private int pos;

  private ImmutableRoaringBitmap roaringBitmap = null;

  /**
   * Creates an instance that is not ready for iteration. You must first call
   * {@link #wrap(ImmutableRoaringBitmap)}.
   */
  public BufferReverseIntIteratorFlyweight() {}

  /**
   * Creates an instance that is ready for iteration.
   *
   * @param r bitmap to be iterated over
   */
  public BufferReverseIntIteratorFlyweight(ImmutableRoaringBitmap r) {
    wrap(r);
  }

  @Override
  public PeekableIntIterator clone() {
    try {
      BufferReverseIntIteratorFlyweight x = (BufferReverseIntIteratorFlyweight) super.clone();
      if (this.iter != null) {
        x.iter = this.iter.clone();
      }
      return x;
    } catch (CloneNotSupportedException e) {
      return null; // will not happen
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

      MappeableContainer container = this.roaringBitmap.highLowContainer.getContainerAtIndex(pos);

      if (container instanceof MappeableBitmapContainer) {
        bitmapIter.wrap((MappeableBitmapContainer) container);
        iter = bitmapIter;
      } else if (container instanceof MappeableRunContainer) {
        runIter.wrap((MappeableRunContainer) container);
        iter = runIter;
      } else {
        arrIter.wrap((MappeableArrayContainer) container);
        iter = arrIter;
      }

      hs = (this.roaringBitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
    }
  }

  /**
   * Prepares a bitmap for iteration
   *
   * @param r bitmap to be iterated over
   */
  public void wrap(ImmutableRoaringBitmap r) {
    this.roaringBitmap = r;
    this.hs = 0;
    this.pos = this.roaringBitmap.highLowContainer.size() - 1;
    this.nextContainer();
  }

  @Override
  public void advanceIfNeeded(int maxval) {
    // In reverse order: skip while next value is strictly greater than maxval (unsigned).
    while (hasNext() && ((hs >>> 16) > (maxval >>> 16))) {
      --pos;
      nextContainer();
    }
    if (hasNext() && ((hs >>> 16) == (maxval >>> 16))) {
      iter.advanceIfNeeded(BufferUtil.lowbits(maxval));
      if (!iter.hasNext()) {
        --pos;
        nextContainer();
      }
    }
  }

  @Override
  public int peekNext() {
    return (iter.peekNext()) | hs;
  }
}
