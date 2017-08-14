/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.longlong;

/**
 * Fast iterator minimizing the stress on the garbage collector. You can create one reusable
 * instance of this class and then {@link #wrap(Roaring64NavigableMap)}
 * 
 * For better performance, consider the {@link Roaring64NavigableMap#forEach} method.
 * 
 * @author Borislav Ivanov
 **/
public class LongIteratorFlyweight implements PeekableLongIterator {

  @Override
  public boolean hasNext() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long next() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void advanceIfNeeded(int minval) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public long peekNext() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public PeekableLongIterator clone() {
    // TODO Auto-generated method stub
    return null;
  }

  public void wrap(Roaring64NavigableMap bitmap_c) {
    // TODO Auto-generated method stub
    
  }

//  private int hs;
//
//  private PeekableShortIterator iter;
//
//  private ArrayContainerShortIterator arrIter = new ArrayContainerShortIterator();
//
//  private BitmapContainerShortIterator bitmapIter = new BitmapContainerShortIterator();
//
//  private RunContainerShortIterator runIter = new RunContainerShortIterator();
//
//  private int pos;
//
//  private RoaringTreeMap roaringBitmap = null;
//
//  /**
//   * Creates an instance that is not ready for iteration. You must first call
//   * {@link #wrap(RoaringBitmap)}.
//   */
//  public LongIteratorFlyweight() {
//
//  }
//
//  /**
//   * Creates an instance that is ready for iteration.
//   * 
//   * @param r bitmap to be iterated over
//   */
//  public LongIteratorFlyweight(RoaringBitmap r) {
//    wrap(r);
//  }
//
//  @Override
//  public PeekableLongIterator clone() {
//    try {
//      LongIteratorFlyweight x = (LongIteratorFlyweight) super.clone();
//      x.iter = this.iter.clone();
//      return x;
//    } catch (CloneNotSupportedException e) {
//      return null;// will not happen
//    }
//  }
//
//  @Override
//  public boolean hasNext() {
//    return pos < this.roaringBitmap.highLowContainer.size();
//  }
//
//  @Override
//  public int next() {
//    int x = iter.nextAsLong() | hs;
//    if (!iter.hasNext()) {
//      ++pos;
//      nextContainer();
//    }
//    return x;
//  }
//
//  private void nextContainer() {
//    if (pos < this.roaringBitmap.highLowContainer.size()) {
//
//      Container container = this.roaringBitmap.highLowContainer.getContainerAtIndex(pos);
//
//      if (container instanceof BitmapContainer) {
//        bitmapIter.wrap(((BitmapContainer) container).bitmap);
//        iter = bitmapIter;
//      } else if (container instanceof ArrayContainer) {
//        arrIter.wrap((ArrayContainer) container);
//        iter = arrIter;
//      } else {
//        runIter.wrap((RunContainer) container);
//        iter = runIter;
//      }
//      hs = Util.toIntUnsigned(this.roaringBitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
//    }
//  }
//
//  /**
//   * Prepares a bitmap for iteration
//   * 
//   * @param r bitmap to be iterated over
//   */
//  public void wrap(RoaringTreeMap r) {
//    this.hs = 0;
//    this.pos = 0;
//    this.roaringBitmap = r;
//    this.nextContainer();
//  }
//
//  @Override
//  public void advanceIfNeeded(final int minval) {
//    while (hasNext() && ((hs >>> 16) < (minval >>> 16))) {
//      ++pos;
//      nextContainer();
//    }
//    if (hasNext() && ((hs >>> 16) == (minval >>> 16))) {
//      iter.advanceIfNeeded(Util.lowbits(minval));
//      if (!iter.hasNext()) {
//        ++pos;
//        nextContainer();
//      }
//    }
//  }
//
//  @Override
//  public int peekNext() {
//    return Util.toIntUnsigned(iter.peekNext()) | hs;
//  }
}
