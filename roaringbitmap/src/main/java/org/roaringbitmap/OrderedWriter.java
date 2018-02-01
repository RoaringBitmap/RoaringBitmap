package org.roaringbitmap;

import java.util.Arrays;


/**
 * 
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
 *       //...
 *
 *       RoaringBitmap r = new RoaringBitmap();
 *       OrderedWriter ow = new OrderedWriter(r);
 *       for (int i :....) {
 *         ow.add(i);
 *       }
 *       ow.flush(); // important
 * }
 * </pre>
 */
public class OrderedWriter {

  private static final int WORD_COUNT = 1 << 10;
  private final long[] bitmap;
  private final RoaringBitmap underlying;

  private short currentKey;
  private boolean dirty = false;

  /**
   * Initialize an OrderedWriter with a receiving bitmap 
   * @param underlying bitmap where the data gets written
   */
  public OrderedWriter(RoaringBitmap underlying) {
    this.underlying = underlying;
    this.bitmap = new long[WORD_COUNT];
  }

  /**
   * Initialize an OrderedWriter and construct a new RoaringBitmap
   */
  public OrderedWriter() {
    this(new RoaringBitmap());
  }
  
  /**
   * Grab a reference to the underlying bitmap
   * @return the underlying bitmap
   */
  public RoaringBitmap getUnderlying(){
    return underlying;
  }

  /**
   * Adds the value to the underlying bitmap. The data might
   * be added to a temporary buffer. You should call "flush"
   * when you are done.
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
