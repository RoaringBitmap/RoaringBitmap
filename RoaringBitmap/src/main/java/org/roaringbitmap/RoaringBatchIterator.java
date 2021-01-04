package org.roaringbitmap;

public final class RoaringBatchIterator implements BatchIterator {

  private final RoaringArray highLowContainer;
  private int index = 0;
  private int key;
  private ContainerBatchIterator iterator;
  private ArrayBatchIterator arrayBatchIterator = null;
  private BitmapBatchIterator bitmapBatchIterator = null;
  private RunBatchIterator runBatchIterator = null;

  public RoaringBatchIterator(RoaringArray highLowContainer) {
    this.highLowContainer = highLowContainer;
    nextIterator();
  }

  @Override
  public int nextBatch(int[] buffer) {
    if (!hasNext()){
      return 0;
    }
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
    if (null != iterator) {
      iterator.releaseContainer();
    }
    if (index < highLowContainer.size()) {
      Container container = highLowContainer.getContainerAtIndex(index);
      if (container instanceof ArrayContainer) {
        nextIterator((ArrayContainer)container);
      } else if (container instanceof BitmapContainer) {
        nextIterator((BitmapContainer)container);
      } else if (container instanceof RunContainer){
        nextIterator((RunContainer)container);
      }
      key = highLowContainer.getKeyAtIndex(index) << 16;
    } else {
      iterator = null;
    }
  }

  private void nextIterator(ArrayContainer array) {
    if (null == arrayBatchIterator) {
      arrayBatchIterator = new ArrayBatchIterator(array);
    } else {
      arrayBatchIterator.wrap(array);
    }
    iterator = arrayBatchIterator;
  }

  private void nextIterator(BitmapContainer bitmap) {
    if (null == bitmapBatchIterator) {
      bitmapBatchIterator = new BitmapBatchIterator(bitmap);
    } else {
      bitmapBatchIterator.wrap(bitmap);
    }
    iterator = bitmapBatchIterator;
  }

  private void nextIterator(RunContainer run) {
    if (null == runBatchIterator) {
      runBatchIterator = new RunBatchIterator(run);
    } else {
      runBatchIterator.wrap(run);
    }
    iterator = runBatchIterator;
  }
}
