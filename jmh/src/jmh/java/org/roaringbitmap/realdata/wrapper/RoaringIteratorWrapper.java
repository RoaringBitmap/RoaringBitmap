package org.roaringbitmap.realdata.wrapper;

import org.roaringbitmap.IntIterator;

final class RoaringIteratorWrapper implements BitmapIterator {

  private final IntIterator iterator;

  RoaringIteratorWrapper(IntIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public int next() {
    return iterator.next();
  }
}
