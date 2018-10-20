package org.roaringbitmap;

import java.util.Arrays;

import static org.roaringbitmap.Util.*;


/**
 * This class can be used to write quickly values to a bitmap.
 * The values are expected to be (increasing) sorted order.
 * Values are first written to a temporary internal buffer, but
 * the underlying bitmap can forcefully synchronize by calling "flush"
 * (although calling flush to often would defeat the performance
 * purpose of this class).
 * The main use case for an ConstantMemoryContainerAppender is to get bitmaps quickly.
 * You should benchmark your particular use case to see if it helps.
 *
 * <pre>
 * {@code
 *
 *       //...
 *
 *
 *       RoaringBitmapWriter<RoaringBitmap> writer =
 *            RoaringBitmapWriter.writer().constantMemory().get();
 *       for (int i :....) {
 *         writer.add(i);
 *       }
 *       writer.flush(); // important
 * }
 * </pre>
 */
public class ConstantMemoryContainerAppender<T extends BitmapDataProvider
        & HasAppendableStorage<Container>> implements Appender<Container, T> {

  private boolean doPartialSort;
  private static final int WORD_COUNT = 1 << 10;
  private final long[] bitmap;
  private final T underlying;
  private boolean dirty = false;
  private short currentKey;

  /**
   * Initialize an ConstantMemoryContainerAppender with a receiving bitmap
   *
   * @param underlying bitmap where the data gets written
   */
  ConstantMemoryContainerAppender(boolean doPartialSort, T underlying) {
    this.underlying = underlying;
    this.doPartialSort = doPartialSort;
    this.bitmap = new long[WORD_COUNT];
  }

  /**
   * Grab a reference to the underlying bitmap
   *
   * @return the underlying bitmap
   */
  @Override
  public T getUnderlying() {
    return underlying;
  }

  /**
   * Adds the value to the underlying bitmap. The data might
   * be added to a temporary buffer. You should call "flush"
   * when you are done.
   *
   * @param value the value to add.
   */
  @Override
  public void add(int value) {
    short key = highbits(value);
    if (key != currentKey) {
      if (compareUnsigned(key, currentKey) < 0) {
        underlying.add(value);
        return;
      } else {
        flush();
      }
      currentKey = key;
    }
    int low = lowbits(value) & 0xFFFF;
    bitmap[(low >>> 6)] |= (1L << low);
    dirty = true;
  }

  @Override
  public void addMany(int... values) {
    if (doPartialSort) {
      partialRadixSort(values);
    }
    for (int value : values) {
      add(value);
    }
  }

  @Override
  public void add(long min, long max) {
    flush();
    underlying.add(min, max);
    short mark = (short)((max >>> 16) + 1);
    if (compareUnsigned(currentKey, mark) < 0) {
      currentKey = mark;
    }
  }

  /**
   * Ensures that any buffered additions are flushed to the underlying bitmap.
   */
  @Override
  public void flush() {
    if (dirty) {
      underlying.getStorage().append(currentKey, chooseBestContainer());
      Arrays.fill(bitmap, 0L);
      dirty = false;
      ++currentKey;
    }
  }

  private Container chooseBestContainer() {
    Container container = new BitmapContainer(bitmap, -1)
            .repairAfterLazy().runOptimize();
    return container instanceof BitmapContainer ? container.clone() : container;
  }
}
