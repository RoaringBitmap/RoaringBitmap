/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.buffer;

import org.roaringbitmap.CharIterator;
import org.roaringbitmap.IntIterator;

/**
 * Fast iterator minimizing the stress on the garbage collector. You can create one reusable
 * instance of this class and then {@link #wrap(ImmutableRoaringBitmap)}
 * 
 * This iterator enumerates the stored values in reverse (starting from the end).
 * 
 * @author Borislav Ivanov
 **/
public class BufferReverseIntIteratorFlyweight implements IntIterator {

  private int hs;

  private CharIterator iter;

  private ReverseMappeableArrayContainerCharIterator arrIter =
      new ReverseMappeableArrayContainerCharIterator();

  private ReverseMappeableBitmapContainerCharIterator bitmapIter =
      new ReverseMappeableBitmapContainerCharIterator();

  private ReverseMappeableRunContainerCharIterator runIter =
      new ReverseMappeableRunContainerCharIterator();

  private short pos;

  private ImmutableRoaringBitmap roaringBitmap = null;


  /**
   * Creates an instance that is not ready for iteration. You must first call
   * {@link #wrap(ImmutableRoaringBitmap)}.
   */
  public BufferReverseIntIteratorFlyweight() {

  }

  /**
   * Creates an instance that is ready for iteration.
   * 
   * @param r bitmap to be iterated over
   */
  public BufferReverseIntIteratorFlyweight(ImmutableRoaringBitmap r) {
    wrap(r);
  }

  @Override
  public IntIterator clone() {
    try {
      BufferReverseIntIteratorFlyweight x = (BufferReverseIntIteratorFlyweight) super.clone();
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
    this.pos = (short) (this.roaringBitmap.highLowContainer.size() - 1);
    this.nextContainer();
  }

}
