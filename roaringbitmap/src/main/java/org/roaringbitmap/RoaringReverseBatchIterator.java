package org.roaringbitmap;

/**
 * Iterates over batches in descending unsigned integer order.
 * The bitmap must not be modified during iteration.
 */
public final class RoaringReverseBatchIterator implements BatchIterator {
  private final RoaringArray containers;
  private int containerIndex;
  private Container container;
  private int key;
  private int position;
  private long word;
  private int cursor;

  public RoaringReverseBatchIterator(RoaringArray containers) {
    this.containers = containers;
    this.containerIndex = containers.size() - 1;
    wrap();
  }

  private void previousContainer() {
    --containerIndex;
    wrap();
  }

  private void wrap() {
    if (containerIndex < 0) {
      container = null;
      return;
    }
    container = containers.getContainerAtIndex(containerIndex);
    key = containers.getKeyAtIndex(containerIndex) << 16;
    if (container instanceof ArrayContainer) {
      position = container.getCardinality() - 1;
    } else if (container instanceof BitmapContainer) {
      position = 1023;
      word = ((BitmapContainer) container).bitmap[position];
    } else {
      RunContainer runs = (RunContainer) container;
      position = runs.numberOfRuns() - 1;
      cursor = runs.getValue(position) + runs.getLength(position);
    }
  }

  private boolean ready() {
    while (container != null) {
      if (container instanceof BitmapContainer) {
        while (word == 0 && position > 0) {
          word = ((BitmapContainer) container).bitmap[--position];
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
      if (container instanceof ArrayContainer) {
        char[] values = ((ArrayContainer) container).content;
        int count = Math.min(buffer.length - used, position + 1);
        for (int i = 0; i < count; i++) {
          buffer[used++] = key | values[position--];
        }
      } else if (container instanceof BitmapContainer) {
        while (word != 0 && used < buffer.length) {
          int bit = 63 - Long.numberOfLeadingZeros(word);
          buffer[used++] = key | (position << 6) | bit;
          word ^= 1L << bit;
        }
      } else {
        RunContainer runs = (RunContainer) container;
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
    if (container instanceof ArrayContainer) {
      ArrayContainer array = (ArrayContainer) container;
      int found = Util.unsignedBinarySearch(array.content, 0, position + 1, (char) low);
      position = found < 0 ? -found - 2 : found;
    } else if (container instanceof BitmapContainer) {
      int targetWord = low >>> 6;
      if (targetWord < position) {
        position = targetWord;
        word = ((BitmapContainer) container).bitmap[position];
      }
      if (targetWord == position) {
        word &= -1L >>> (63 - (low & 63));
      }
    } else {
      RunContainer runs = (RunContainer) container;
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
      return (RoaringReverseBatchIterator) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }
}
