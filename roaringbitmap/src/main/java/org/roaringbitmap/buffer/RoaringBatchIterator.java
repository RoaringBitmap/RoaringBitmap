package org.roaringbitmap.buffer;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.ContainerBatchIterator;

public class RoaringBatchIterator implements BatchIterator {

  private MappeableContainerPointer containerPointer;
  int index = 0;
  int key;
  ContainerBatchIterator iterator;

  public RoaringBatchIterator(MappeableContainerPointer containerPointer) {
    this.containerPointer = containerPointer;
    nextIterator();
  }

  @Override
  public int nextBatch(int[] buffer) {
    int consumed = 0;
    if (iterator.hasNext()) {
      consumed += iterator.next(key, buffer);
      if (consumed > 0) {
        return consumed;
      }
    }
    containerPointer.advance();
    nextIterator();
    if (null != iterator) {
      return nextBatch(buffer);
    }
    return consumed;
  }

  @Override
  public boolean hasNext() {
    return null != iterator;
  }

  @Override
  public BatchIterator clone() {
    try {
      RoaringBatchIterator it = (RoaringBatchIterator)super.clone();
      if (null != iterator) {
        it.iterator = iterator.clone();
      }
      if (null != containerPointer) {
        it.containerPointer = containerPointer.clone();
      }
      return it;
    } catch (CloneNotSupportedException e) {
      // won't happen
      throw new IllegalStateException();
    }
  }

  private void nextIterator() {
    if (null != containerPointer && containerPointer.hasContainer()) {
      iterator = containerPointer.getContainer().getBatchIterator();
      key = containerPointer.key() << 16;
    } else {
      iterator = null;
    }
  }
}
