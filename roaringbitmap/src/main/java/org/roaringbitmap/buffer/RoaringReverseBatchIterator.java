package org.roaringbitmap.buffer;

/**
 * Iterates over batches in descending unsigned integer order.
 * The bitmap must not be modified during iteration.
 */
public final class RoaringReverseBatchIterator implements org.roaringbitmap.BatchIterator {
  private MappeableContainerPointer containers;
  private MappeableContainer container;
  private int key;
  private int position;
  private long word;
  private int cursor;

  public RoaringReverseBatchIterator(MappeableContainerPointer containers) {
    this.containers = containers;
    wrap();
  }

  private void previousContainer() {
    containers.previous();
    wrap();
  }

  private void wrap() {
    if (containers == null || !containers.hasContainer()) {
      container = null;
      return;
    }
    container = containers.getContainer();
    key = containers.key() << 16;
    if (container instanceof MappeableArrayContainer) {
      position = container.getCardinality() - 1;
    } else if (container instanceof MappeableBitmapContainer) {
      position = 1023;
      word = ((MappeableBitmapContainer) container).bitmap.get(position);
    } else {
      MappeableRunContainer runs = (MappeableRunContainer) container;
      position = runs.numberOfRuns() - 1;
      cursor = runs.getValue(position) + runs.getLength(position);
    }
  }

  private boolean ready() {
    while (container != null) {
      if (container instanceof MappeableBitmapContainer) {
        while (word == 0 && position > 0) {
          word = ((MappeableBitmapContainer) container).bitmap.get(--position);
        }
        if (word != 0) {
          return true;
        }
      } else if (position >= 0) {
        return true;
      }
      previousContainer();
    }
    return false;
  }

  @Override
  public boolean hasNext() {
    return ready();
  }

  @Override
  public int nextBatch(int[] buffer) {
    int used = 0;
    while (used < buffer.length && ready()) {
      if (container instanceof MappeableArrayContainer) {
        java.nio.CharBuffer values = ((MappeableArrayContainer) container).content;
        int count = Math.min(buffer.length - used, position + 1);
        for (int i = 0; i < count; i++) {
          buffer[used++] = key | values.get(position--);
        }
      } else if (container instanceof MappeableBitmapContainer) {
        while (word != 0 && used < buffer.length) {
          int bit = 63 - Long.numberOfLeadingZeros(word);
          buffer[used++] = key | (position << 6) | bit;
          word ^= 1L << bit;
        }
      } else {
        MappeableRunContainer runs = (MappeableRunContainer) container;
        int count = Math.min(buffer.length - used, cursor - runs.getValue(position) + 1);
        for (int i = 0; i < count; i++) {
          buffer[used++] = key | cursor--;
        }
        if (cursor < runs.getValue(position) && --position >= 0) {
          cursor = runs.getValue(position) + runs.getLength(position);
        }
      }
    }
    return used;
  }

  /** Skip unsigned values greater than target, never rewinding. */
  @Override
  public void advanceIfNeeded(int target) {
    while (ready() && (key >>> 16) > (target >>> 16)) {
      previousContainer();
    }
    if (!ready() || (key >>> 16) != (target >>> 16)) {
      return;
    }
    int low = target & 0xffff;
    if (container instanceof MappeableArrayContainer) {
      MappeableArrayContainer array = (MappeableArrayContainer) container;
      int found = BufferUtil.unsignedBinarySearch(array.content, 0, position + 1, (char) low);
      position = found < 0 ? -found - 2 : found;
    } else if (container instanceof MappeableBitmapContainer) {
      int targetWord = low >>> 6;
      if (targetWord < position) {
        position = targetWord;
        word = ((MappeableBitmapContainer) container).bitmap.get(position);
      }
      if (targetWord == position) {
        word &= -1L >>> (63 - (low & 63));
      }
    } else {
      MappeableRunContainer runs = (MappeableRunContainer) container;
      while (position >= 0 && runs.getValue(position) > low) {
        --position;
      }
      if (position >= 0) {
        cursor =
            Math.min(cursor, Math.min(low, runs.getValue(position) + runs.getLength(position)));
      }
    }
  }

  @Override
  public RoaringReverseBatchIterator clone() {
    try {
      RoaringReverseBatchIterator copy = (RoaringReverseBatchIterator) super.clone();
      copy.containers = containers == null ? null : containers.clone();
      return copy;
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }
}
