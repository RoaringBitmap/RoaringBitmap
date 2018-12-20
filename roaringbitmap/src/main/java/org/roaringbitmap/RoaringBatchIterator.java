package org.roaringbitmap;

public class RoaringBatchIterator implements BatchIterator {

  private final RoaringArray highLowContainer;
  int index = 0;
  int key;
  ContainerBatchIterator iterator;

  public RoaringBatchIterator(RoaringArray highLowContainer) {
    this.highLowContainer = highLowContainer;
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
    ++index;
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
      return it;
    } catch (CloneNotSupportedException e) {
      // won't happen
      throw new IllegalStateException();
    }
  }

  private void nextIterator() {
    if (index < highLowContainer.size()) {
      iterator = highLowContainer.getContainerAtIndex(index).getBatchIterator();
      key = highLowContainer.getKeyAtIndex(index) << 16;
    } else {
      iterator = null;
    }
  }
}
