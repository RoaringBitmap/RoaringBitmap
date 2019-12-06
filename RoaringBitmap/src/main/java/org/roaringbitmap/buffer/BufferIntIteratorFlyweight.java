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
 * For better performance, consider the {@link ImmutableRoaringBitmap#forEach} method.
 *
 * @author Borislav Ivanov
 **/
public class BufferIntIteratorFlyweight implements PeekableIntIterator {

  private int hs;

  private PeekableCharIterator iter;

  private MappeableArrayContainerCharIterator arrIter = new MappeableArrayContainerCharIterator();

  private MappeableBitmapContainerCharIterator bitmapIter =
      new MappeableBitmapContainerCharIterator();

  private MappeableRunContainerCharIterator runIter = new MappeableRunContainerCharIterator();


  private int pos;

  private ImmutableRoaringBitmap roaringBitmap = null;

  /**
   * Creates an instance that is not ready for iteration. You must first call
   * {@link #wrap(ImmutableRoaringBitmap)}.
   */
  public BufferIntIteratorFlyweight() {

  }

  /**
   * Creates an instance that is ready for iteration.
   * 
   * @param r bitmap to be iterated over
   */
  public BufferIntIteratorFlyweight(ImmutableRoaringBitmap r) {
    wrap(r);
  }

  @Override
  public PeekableIntIterator clone() {
    try {
      BufferIntIteratorFlyweight x = (BufferIntIteratorFlyweight) super.clone();
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
    return pos < this.roaringBitmap.highLowContainer.size();
  }

  @Override
  public int next() {
    int x = iter.nextAsInt() | hs;
    if (!iter.hasNext()) {
      ++pos;
      nextContainer();
    }
    return x;
  }

  private void nextContainer() {
    if (pos < this.roaringBitmap.highLowContainer.size()) {

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
    this.hs = 0;
    this.pos = 0;
    this.roaringBitmap = r;
    this.nextContainer();
  }

  @Override
  public void advanceIfNeeded(int minval) {
    while (hasNext() && ((hs >>> 16) < (minval >>> 16))) {
      ++pos;
      nextContainer();
    }
    if (hasNext() && ((hs >>> 16) == (minval >>> 16))) {
      iter.advanceIfNeeded(BufferUtil.lowbits(minval));
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
