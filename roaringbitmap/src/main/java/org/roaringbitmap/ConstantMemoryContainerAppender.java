package org.roaringbitmap;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Long.numberOfTrailingZeros;
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
        & AppendableStorage<Container>> implements RoaringBitmapWriter<T> {

  private boolean doPartialSort;
  private static final int WORD_COUNT = 1 << 10;
  private final long[] bitmap;
  private final Supplier<T> newUnderlying;
  private T underlying;
  private boolean dirty = false;
  private int currentKey;

  /**
   * Initialize an ConstantMemoryContainerAppender with a receiving bitmap
   *
   * @param doPartialSort indicates whether to sort the upper 16 bits of input data in addMany
   * @param newUnderlying supplier of bitmaps where the data gets written
   */
  ConstantMemoryContainerAppender(boolean doPartialSort, Supplier<T> newUnderlying) {
    this.newUnderlying = newUnderlying;
    this.underlying = newUnderlying.get();
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
    int low = toIntUnsigned(lowbits(value));
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
    appendToUnderlying();
    underlying.add(min, max);
    int mark = (int)((max >>> 16) + 1);
    if (currentKey < mark) {
      currentKey = mark;
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
      int low = toIntUnsigned(lowbits(value));
      return (bitmap[(low >>> 6)] & (1L << low)) != 0;
    } else {
      return underlying.contains(value);
    }
  }

  @Override
  public int getCardinality() {
    return (int) (underlying.getLongCardinality() + computeCardinality(bitmap));
  }

  @Override
  public boolean isEmpty() {
    if (underlying.isEmpty()) {
      for (long w : bitmap) {
        if (w != 0) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int first() {
    if (underlying.isEmpty()) {
      int first = 0;
      for (int i = 0; i < bitmap.length; ++i) {
        if (bitmap[i] == 0) {
          first += Long.SIZE;
        } else {
          first += numberOfTrailingZeros(bitmap[i]);
          break;
        }
      }
      if (first == 0x10000) {
        throw new NoSuchElementException("Empty");
      }
      return (currentKey << 16) | first;
    }
    return underlying.first();
  }

  @Override
  public int last() {
    int last = 0x10000;
    for (int i = bitmap.length - 1; i >= 0; --i) {
      if (bitmap[i] == 0) {
        last -= Long.SIZE;
      } else {
        last -= (numberOfLeadingZeros(bitmap[i]) + 1);
        break;
      }
    }
    if (last == 0) {
      return underlying.last();
    }
    return (currentKey << 16) | last;
  }

  @Override
  public void reset() {
    currentKey = 0;
    underlying = newUnderlying.get();
    dirty = false;
  }

  private Container chooseBestContainer() {
    Container container = new BitmapContainer(bitmap, -1)
            .repairAfterLazy().runOptimize();
    return container instanceof BitmapContainer ? container.clone() : container;
  }

  private int appendToUnderlying() {
    if (dirty) {
      assert currentKey <= 0xFFFF;
      underlying.append((short) currentKey, chooseBestContainer());
      Arrays.fill(bitmap, 0L);
      dirty = false;
      return 1;
    }
    return 0;
  }
}
