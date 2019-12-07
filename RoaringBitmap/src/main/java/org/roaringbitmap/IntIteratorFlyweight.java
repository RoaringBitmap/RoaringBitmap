/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * Fast iterator minimizing the stress on the garbage collector. You can create one reusable
 * instance of this class and then {@link #wrap(RoaringBitmap)}
 * 
 * For better performance, consider the {@link RoaringBitmap#forEach} method.
 * 
 * @author Borislav Ivanov
 **/
public class IntIteratorFlyweight implements PeekableIntIterator {

  private int hs;

  private PeekableCharIterator iter;

  private ArrayContainerCharIterator arrIter = new ArrayContainerCharIterator();

  private BitmapContainerCharIterator bitmapIter = new BitmapContainerCharIterator();

  private RunContainerCharIterator runIter = new RunContainerCharIterator();

  private int pos;

  private RoaringBitmap roaringBitmap = null;

  /**
   * Creates an instance that is not ready for iteration. You must first call
   * {@link #wrap(RoaringBitmap)}.
   */
  public IntIteratorFlyweight() {

  }

  /**
   * Creates an instance that is ready for iteration.
   * 
   * @param r bitmap to be iterated over
   */
  public IntIteratorFlyweight(RoaringBitmap r) {
    wrap(r);
  }

  @Override
  public PeekableIntIterator clone() {
    try {
      IntIteratorFlyweight x = (IntIteratorFlyweight) super.clone();
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

      Container container = this.roaringBitmap.highLowContainer.getContainerAtIndex(pos);

      if (container instanceof BitmapContainer) {
        bitmapIter.wrap(((BitmapContainer) container).bitmap);
        iter = bitmapIter;
      } else if (container instanceof ArrayContainer) {
        arrIter.wrap((ArrayContainer) container);
        iter = arrIter;
      } else {
        runIter.wrap((RunContainer) container);
        iter = runIter;
      }
      hs = (this.roaringBitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
    }
  }

  /**
   * Prepares a bitmap for iteration
   * 
   * @param r bitmap to be iterated over
   */
  public void wrap(RoaringBitmap r) {
    this.hs = 0;
    this.pos = 0;
    this.roaringBitmap = r;
    this.nextContainer();
  }

  @Override
  public void advanceIfNeeded(final int minval) {
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
