package org.roaringbitmap.buffer;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.ContainerBatchIterator;

public final class RoaringBatchIterator implements BatchIterator {

  private MappeableContainerPointer containerPointer;
  private int key;
  private ContainerBatchIterator iterator;
  private ArrayBatchIterator arrayBatchIterator;
  private BitmapBatchIterator bitmapBatchIterator;
  private RunBatchIterator runBatchIterator;

  public RoaringBatchIterator(MappeableContainerPointer containerPointer) {
    this.containerPointer = containerPointer;
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
    if (null != iterator) {
      iterator.releaseContainer();
    }
    if (null != containerPointer && containerPointer.hasContainer()) {
      MappeableContainer container = containerPointer.getContainer();
      if (container instanceof MappeableArrayContainer) {
        nextIterator((MappeableArrayContainer)container);
      } else if (container instanceof MappeableBitmapContainer) {
        nextIterator((MappeableBitmapContainer)container);
      } else if (container instanceof MappeableRunContainer) {
        nextIterator((MappeableRunContainer)container);
      }
      key = containerPointer.key() << 16;
    } else {
      iterator = null;
    }
  }

  private void nextIterator(MappeableArrayContainer array) {
    if (null == arrayBatchIterator) {
      arrayBatchIterator = new ArrayBatchIterator(array);
    } else {
      arrayBatchIterator.wrap(array);
    }
    iterator = arrayBatchIterator;
  }

  private void nextIterator(MappeableBitmapContainer bitmap) {
    if (null == bitmapBatchIterator) {
      bitmapBatchIterator = new BitmapBatchIterator(bitmap);
    } else {
      bitmapBatchIterator.wrap(bitmap);
    }
    iterator = bitmapBatchIterator;
  }

  private void nextIterator(MappeableRunContainer run) {
    if (null == runBatchIterator) {
      runBatchIterator = new RunBatchIterator(run);
    } else {
      runBatchIterator.wrap(run);
    }
    iterator = runBatchIterator;
  }
}
