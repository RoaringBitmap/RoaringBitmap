package org.roaringbitmap.realdata.wrapper;

import io.druid.extendedset.intset.IntSet;

final class ConciseSetIteratorWrapper implements BitmapIterator {

  private final IntSet.IntIterator iterator;

  ConciseSetIteratorWrapper(IntSet.IntIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Integer next() {
    return iterator.next();
  }
  
  @Override
  public int nextInt() {
    return iterator.next();
  }

}
