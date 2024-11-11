package org.roaringbitmap.realdata.wrapper;

import com.googlecode.javaewah.IntIterator;

final class EwahIteratorWrapper implements BitmapIterator {

  private final IntIterator iterator;

  EwahIteratorWrapper(IntIterator iterator) {
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
