package org.roaringbitmap;

import java.util.Arrays;

public class OrderedWriter {

  private static final int WORD_COUNT = 1 << 10;
  private final long[] bitmap;
  private final RoaringBitmap underlying;

  private short currentKey;
  private boolean dirty = false;

  public OrderedWriter(RoaringBitmap underlying) {
    this.underlying = underlying;
    this.bitmap = new long[WORD_COUNT];
  }

  /**
   * Adds the value to the underlying bitmap.
   * @param value the value to add.
   */
  public void add(int value) {
    short key = Util.highbits(value);
    short low = Util.lowbits(value);
    if (key != currentKey) {
      if (Util.compareUnsigned(key, currentKey) < 0) {
        throw new IllegalStateException("Must write in ascending key order");
      }
      flush();
    }
    int ulow = low & 0xFFFF;
    bitmap[(ulow >>> 6)] |= (1L << ulow);
    currentKey = key;
    dirty = true;
  }

  /**
   * Ensures that any buffered additions are flushed to the underlying bitmap.
   */
  public void flush() {
    if (dirty) {
      RoaringArray highLowContainer = underlying.highLowContainer;
      // we check that it's safe to append since RoaringArray.append does no validation
      if (highLowContainer.size > 0) {
        short key = highLowContainer.getKeyAtIndex(highLowContainer.size - 1);
        if (Util.compareUnsigned(currentKey, key) <= 0) {
          throw new IllegalStateException("Cannot write " + currentKey + " after " + key);
        }
      }
      highLowContainer.append(currentKey, chooseBestContainer());
      clearBitmap();
      dirty = false;
    }
  }

  private Container chooseBestContainer() {
    Container container = new BitmapContainer(bitmap,-1).repairAfterLazy().runOptimize();
    return container instanceof BitmapContainer ? container.clone() : container;
  }

  private void clearBitmap() {
    Arrays.fill(bitmap, 0L);
  }
}
