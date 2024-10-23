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
        implements RoaringBitmapWriter<T> {


  private final boolean doPartialSort;
  private final boolean runCompress;
  private final Supplier<C> newContainer;
  private final Supplier<T> newUnderlying;
  private C container;
  private T underlying;
  private int currentKey;

  /**
   * Initialize an ContainerAppender with a receiving bitmap
   *
   */
  ContainerAppender(boolean doPartialSort,
                    boolean runCompress,
                    Supplier<T> newUnderlying,
                    Supplier<C> newContainer) {
    this.doPartialSort = doPartialSort;
    this.runCompress = runCompress;
    this.newUnderlying = newUnderlying;
    this.underlying = newUnderlying.get();
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
    int key = (highbits(value));
    if (key != currentKey) {
      if (key < currentKey) {
        underlying.add(value);
        return;
      } else {
        appendToUnderlying();
        currentKey = key;
      }
    }
    C tmp = container.add(lowbits(value));
    if (tmp != container) {
      container = tmp;
    }
  }

  @Override
  public void add(long min, long max) {
    appendToUnderlying();
    underlying.add(min, max);
    int mark = (int)((max >>> 16) + 1);
    if (currentKey < mark) {
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
    currentKey += appendToUnderlying();
  }

  @Override
  public void reset() {
    currentKey = 0;
    container = newContainer.get();
    underlying = newUnderlying.get();
  }

  private int appendToUnderlying() {
    if (!container.isEmpty()) {
      assert currentKey <= 0xFFFF;
      underlying.append((char) currentKey,
              runCompress ? container.runOptimize() : container);
      container = newContainer.get();
      return 1;
    }
    return 0;
  }
}