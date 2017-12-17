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
    if (Util.compareUnsigned(key, currentKey) < 0) {
      throw new IllegalStateException("Must write in ascending key order");
    }
    if (key != currentKey) {
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
      int cardinality = computeCardinality();
      underlying.highLowContainer.append(currentKey, chooseBestContainer(cardinality));
      clearBitmap();
      dirty = false;
    }
  }

  private Container chooseBestContainer(int cardinality) {
    if (cardinality < WORD_COUNT) {
      return createArrayContainer(cardinality);
    }
    Container bitmapContainer = new BitmapContainer(bitmap, cardinality);
    Container runOptimised = bitmapContainer.runOptimize();
    if (runOptimised == bitmapContainer) { // don't let our array escape
      return new BitmapContainer(Arrays.copyOf(bitmap, bitmap.length), cardinality);
    }
    return runOptimised;
  }

  private ArrayContainer createArrayContainer(int cardinality) {
    short[] array = new short[cardinality];
    int arrIndex = 0;
    for (int i = 0; i < bitmap.length; ++i) {
      long word = bitmap[i];
      for (int j = Long.numberOfTrailingZeros(word); j < 64; j = Long.numberOfTrailingZeros(word)) {
        if ((word & (1L << j)) != 0) {
          word ^= (1L << j);
          array[arrIndex++] = (short) ((i << 6) + j);
        }
      }
    }
    return new ArrayContainer(cardinality, array);
  }

  private void clearBitmap() {
    Arrays.fill(bitmap, 0L);
  }

  private int computeCardinality() {
    int cardinality = 0;
    for (int i = 0; i < bitmap.length; ++i) {
      cardinality += Long.bitCount(bitmap[i]);
    }
    return cardinality;
  }
}
