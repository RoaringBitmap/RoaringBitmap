package org.roaringbitmap.buffer;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.ContainerBatchIterator;

public class RoaringBatchIterator implements BatchIterator {

  private final MappeableContainerPointer containerPointer;
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
    } else {
      containerPointer.advance();
      nextIterator();
      if (null != iterator) {
        return nextBatch(buffer);
      }
    }
    return consumed;
  }

  @Override
  public boolean hasNext() {
    return null != iterator;
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
