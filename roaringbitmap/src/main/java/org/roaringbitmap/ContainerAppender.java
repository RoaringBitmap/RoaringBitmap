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
  private final Supplier<C> newContainer;
  private final Supplier<T> newUnderlying;
  private C container;
  private T underlying;
  private int currentKey;

  /**
   * Initialize an ContainerAppender with a receiving bitmap
   *
   */
  ContainerAppender(boolean doPartialSort, Supplier<T> newUnderlying, Supplier<C> newContainer) {
    this(doPartialSort, newUnderlying, newContainer, newUnderlying.get());
  }

  private ContainerAppender(boolean doPartialSort, Supplier<T> newUnderlying, Supplier<C> newContainer, T underlying) {
    this.doPartialSort = doPartialSort;
    this.newUnderlying = newUnderlying;
    this.underlying = underlying;
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
    int key = toIntUnsigned(highbits(value));
    if (key != currentKey) {
      if (key < currentKey) {
        underlying.add(value);
        return;
      } else {
        appendToUnderlying();
        currentKey = key;
      }
    }
    setIfDifferent(container.add(lowbits(value)));
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
  public boolean contains(int value) {
    if (currentKey == value >>> 16) {
      return container.contains(lowbits(value));
    } else {
      return underlying.contains(value);
    }
  }

  @Override
  public int getCardinality() {
    return (int) (underlying.getLongCardinality() + container.getCardinality());
  }

  @Override
  public long getLongCardinality() {
    return underlying.getLongCardinality() + container.getCardinality();
  }

  @Override
  public void remove(int x) {
    if (currentKey == x >>> 16) {
      setIfDifferent(container.remove(lowbits(x)));
    } else {
      underlying.remove(x);
    }
  }

  @Override
  public void trim() {
    underlying.trim();
  }

  @Override
  public boolean isEmpty() {
    return underlying.isEmpty() && container.isEmpty();
  }

  @Override
  public int first() {
    if (underlying.isEmpty()) {
      return (currentKey << 16) | container.first();
    }
    return underlying.first();
  }

  @Override
  public int last() {
    if (container.isEmpty()) {
      return underlying.last();
    }
    return (currentKey << 16) | container.last();
  }

  @Override
  public long getLongSizeInBytes() {
    return underlying.getLongSizeInBytes() + container.getSizeInBytes();
  }

  @Override
  public ImmutableBitmapDataProvider limit(int x) {
    if (Integer.compareUnsigned(currentKey, x) > 0) {
      flush();
    }
    // TODO return new ContainerAppender<>(doPartialSort, newUnderlying, newContainer, underlying.limit(x));
    return underlying.limit(x);
  }

  @Override
  public long rankLong(int x) {
    int high = x >>> 16;
    if (high > currentKey) {
      return getLongCardinality();
    } else if (high == currentKey) {
      return underlying.getLongCardinality() + container.rank(lowbits(x));
    } else {
      return underlying.rankLong(x);
    }
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
      underlying.append((short)currentKey, container.runOptimize());
      container = newContainer.get();
      return 1;
    }
    return 0;
  }

  private void setIfDifferent(C newContainer) {
    if (newContainer != container) {
      container = newContainer;
    }
  }
}