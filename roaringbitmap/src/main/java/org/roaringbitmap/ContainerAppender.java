package org.roaringbitmap;


import java.util.function.Supplier;

import static org.roaringbitmap.Util.*;

/**
 * This class can be used to write quickly values to a bitmap.
 * The values are expected to be (increasing) sorted order.
 * Values are first written to a temporary internal buffer, but
 * the underlying bitmap can forcefully synchronize by calling "flush"
 * (although calling flush to often would defeat the performance
 * purpose of this class).
 * The main use case for an ContainerAppender is to get bitmaps quickly.
 * You should benchmark your particular use case to see if it helps.
 *
 * <pre>
 * {@code
 *
 *     //...
 *
 *     RoaringBitmapWriter<RoaringBitmap> writer =
 *        RoaringBitmapWriter.writer().get();
 *     for (int i :....) {
 *       writer.add(i);
 *     }
 *     writer.flush(); // important
 * }
 * </pre>
 */
public class ContainerAppender<C extends WordStorage<C>,
        T extends BitmapDataProvider & AppendableStorage<C>>
        implements Appender<C, T> {


  final private boolean doPartialSort;
  private final T underlying;
  private final Supplier<C> newContainer;
  private C container;
  private short currentKey;

  /**
   * Initialize an ContainerAppender with a receiving bitmap
   *
   */
  ContainerAppender(boolean doPartialSort, T bitmap, Supplier<C> newContainer) {
    this.doPartialSort = doPartialSort;
    this.underlying = bitmap;
    this.newContainer = newContainer;
    this.container = newContainer.get();
  }

  /**
   * Grab a reference to the underlying bitmap
   *
   * @return the underlying bitmap
   */
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
    container = container.add(lowbits(value));
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

  @Override
  public void addMany(int... values) {
    if (doPartialSort) {
      partialRadixSort(values);
    }
    for (int i : values) {
      add(i);
    }
  }

  /**
   * Ensures that any buffered additions are flushed to the underlying bitmap.
   */
  @Override
  public void flush() {
    if (!container.isEmpty()) {
      underlying.append(currentKey, container.runOptimize());
      ++currentKey;
      container = newContainer.get();
    }
  }
}