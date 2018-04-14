package org.roaringbitmap;

import static org.roaringbitmap.Util.toIntUnsigned;

public class ArrayBatchIterator implements ContainerBatchIterator {

  private int index = 0;
  private final ArrayContainer array;

  public ArrayBatchIterator(ArrayContainer array) {
    this.array = array;
  }

  @Override
  public int next(int key, int[] buffer) {
    int consumed = 0;
    short[] data = array.content;
    while (consumed < buffer.length && index < array.getCardinality()) {
      buffer[consumed++] = key + toIntUnsigned(data[index++]);
    }
    return consumed;
  }

  @Override
  public boolean hasNext() {
    return index < array.getCardinality();
  }
}
