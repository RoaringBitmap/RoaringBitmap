package org.roaringbitmap.buffer;

import org.roaringbitmap.ContainerBatchIterator;

import java.nio.ShortBuffer;

import static org.roaringbitmap.buffer.BufferUtil.toIntUnsigned;

public class ArrayBatchIterator implements ContainerBatchIterator {

  private int index = 0;
  private final MappeableArrayContainer array;

  public ArrayBatchIterator(MappeableArrayContainer array) {
    this.array = array;
  }

  @Override
  public int next(int key, int[] buffer) {
    int consumed = 0;
    ShortBuffer data = array.content;
    while (consumed < buffer.length && index < array.getCardinality()) {
      buffer[consumed++] = key + toIntUnsigned(data.get(index++));
    }
    return consumed;
  }

  @Override
  public boolean hasNext() {
    return index < array.getCardinality();
  }
}
