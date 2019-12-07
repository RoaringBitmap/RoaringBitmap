package org.roaringbitmap.buffer;

import org.roaringbitmap.ContainerBatchIterator;

import java.nio.CharBuffer;



public final class ArrayBatchIterator implements ContainerBatchIterator {

  private int index = 0;
  private MappeableArrayContainer array;

  public ArrayBatchIterator(MappeableArrayContainer array) {
    wrap(array);
  }

  @Override
  public int next(int key, int[] buffer) {
    int consumed = 0;
    CharBuffer data = array.content;
    while (consumed < buffer.length && index < array.getCardinality()) {
      buffer[consumed++] = key + (data.get(index++));
    }
    return consumed;
  }

  @Override
  public boolean hasNext() {
    return index < array.getCardinality();
  }

  @Override
  public ContainerBatchIterator clone() {
    try {
      return (ContainerBatchIterator)super.clone();
    } catch (CloneNotSupportedException e) {
      // won't happen
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void releaseContainer() {
    array = null;
  }

  public void wrap(MappeableArrayContainer array) {
    this.array = array;
    this.index = 0;
  }
}
