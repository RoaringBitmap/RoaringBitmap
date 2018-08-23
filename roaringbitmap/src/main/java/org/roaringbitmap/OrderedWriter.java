package org.roaringbitmap;


import java.util.Arrays;

/**
 * This class can be used to write quickly values to a bitmap.
 * The values are expected to be (increasing) sorted order.
 * Values are first written to a temporary internal buffer, but
 * the underlying bitmap can forcefully synchronize by calling "flush"
 * (although calling flush to often would defeat the performance
 * purpose of this class).
 * The main use case for an OrderedWriter is to create bitmaps quickly.
 * You should benchmark your particular use case to see if it helps.
 *
 * <pre>
 * {@code
 *
 *     //...
 *
 *     RoaringBitmap r = new RoaringBitmap();
 *     OrderedWriter ow = new OrderedWriter(r);
 *     for (int i :....) {
 *     ow.add(i);
 *     }
 *     ow.flush(); // important
 * }
 * </pre>
 */
public class OrderedWriter {

  private static final int BITMAP_WORD_COUNT = 1 << 10;
  private static final int ARRAY_MAX_SIZE = ArrayContainer.DEFAULT_MAX_SIZE;

  private final RoaringBitmap underlying;
  private long[] bitmap;
  private short[] values;
  private int valuesCount;
  private short currentKey;

  /**
   * Initialize an OrderedWriter with a receiving bitmap
   *
   * @param underlying bitmap where the data gets written
   */
  public OrderedWriter(RoaringBitmap underlying) {
    this.underlying = underlying;
    this.bitmap = new long[BITMAP_WORD_COUNT];
    this.valuesCount = 0;
    this.values = new short[ARRAY_MAX_SIZE];
  }

  /**
   * Initialize an OrderedWriter and construct a new RoaringBitmap
   */
  public OrderedWriter() {
    this(new RoaringBitmap());
  }

  /**
   * Grab a reference to the underlying bitmap
   *
   * @return the underlying bitmap
   */
  public RoaringBitmap getUnderlying() {
    return underlying;
  }

  /**
   * Adds the value to the underlying bitmap. The data might
   * be added to a temporary buffer. You should call "flush"
   * when you are done.
   *
   * @param value the value to add.
   * @throws IllegalStateException if values are not added in increasing order
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
    int bitmapIndex = ulow >>> 6;
    long oldBitmapWord = bitmap[bitmapIndex];
    bitmap[bitmapIndex] |= (1L << ulow);

    if (oldBitmapWord == bitmap[bitmapIndex]) {
      // Nothing changed.
      return;
    }

    currentKey = key;

    int valueIndex = valuesCount++;
    if (valueIndex < values.length) {
      values[valueIndex] = low;
    }
  }


  /**
   * Ensures that any buffered additions are flushed to the underlying bitmap.
   */
  public void flush() {
    if (valuesCount > 0) {
      RoaringArray highLowContainer = underlying.highLowContainer;
      // we check that it's safe to append since RoaringArray.append does no validation
      if (highLowContainer.size > 0) {
        short key = highLowContainer.getKeyAtIndex(highLowContainer.size - 1);
        if (Util.compareUnsigned(currentKey, key) <= 0) {
          throw new IllegalStateException("Cannot write " + currentKey + " after " + key);
        }
      }
      highLowContainer.append(currentKey, buildBestContainer());
    }
  }

  private Container buildBestContainer() {
    boolean allocatedNewArray = false;
    boolean allocatedNewBitmap = false;

    Container container;
    if (valuesCount <= values.length) {
      short[] values;
      if (valuesCount == ARRAY_MAX_SIZE) {
        values = this.values;
      } else {
        values = Arrays.copyOf(this.values, valuesCount);
      }
      container = new ArrayContainer(values).runOptimize();
      if (container instanceof ArrayContainer) {
        if (((ArrayContainer) container).content == this.values) {
          this.values = new short[ARRAY_MAX_SIZE];
          allocatedNewArray = true;
        }
      }
    } else {
      container = new BitmapContainer(bitmap, valuesCount).runOptimize();
      if (container instanceof BitmapContainer) {
        if (((BitmapContainer) container).bitmap == bitmap) {
          bitmap = new long[BITMAP_WORD_COUNT];
          allocatedNewBitmap = true;
        }
      }
    }

    valuesCount = 0;
    if (!allocatedNewBitmap) {
      Arrays.fill(bitmap, 0);
    }
    if (!allocatedNewArray) {
      Arrays.fill(values, (short) 0);
    }

    return container;
  }
}
