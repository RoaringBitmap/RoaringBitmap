package org.roaringbitmap;


/**
 * This class can be used to write quickly values to a bitmap.
 * The values are expected to be (increasing) sorted order.
 * Values are first written to a temporary internal buffer, but
 * the underlying bitmap can forcefully synchronize by calling "flush"
 * (although calling flush to often would defeat the performance
 * purpose of this class).
 * The main use case for an SparseOrderedWriter is to create bitmaps quickly.
 * You should benchmark your particular use case to see if it helps.
 *
 * <pre>
 * {@code
 *
 *     //...
 *
 *     RoaringBitmap r = new RoaringBitmap();
 *     SparseOrderedWriter ow = new SparseOrderedWriter(r);
 *     for (int i :....) {
 *     ow.add(i);
 *     }
 *     ow.flush(); // important
 * }
 * </pre>
 */
public class SparseOrderedWriter implements OrderedWriter {


  private final RoaringBitmap underlying;
  private Container container;
  private short currentKey;

  /**
   * Initialize an SparseOrderedWriter with a receiving bitmap
   *
   * @param underlying bitmap where the data gets written
   */
  public SparseOrderedWriter(RoaringBitmap underlying) {
    this.underlying = underlying;
    this.container = new ArrayContainer();
  }


  /**
   * Initialize an SparseOrderedWriter and construct a new RoaringBitmap
   */
  public SparseOrderedWriter() {
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
   */
  @Override
  public void add(int value) {
    short key = Util.highbits(value);
    short low = Util.lowbits(value);
    if (key != currentKey) {
      if (Util.compareUnsigned(key, currentKey) < 0) {
        underlying.add(value);
        return;
      } else {
        flush();
      }
    }
    currentKey = key;
    container = container.add(low);
  }

  /**
   * Ensures that any buffered additions are flushed to the underlying bitmap.
   */
  @Override
  public void flush() {
    if (!container.isEmpty()) {
      RoaringArray highLowContainer = underlying.highLowContainer;
      // we check that it's safe to append since RoaringArray.append does no validation
      if (highLowContainer.size > 0) {
        short key = highLowContainer.getKeyAtIndex(highLowContainer.size - 1);
        if (Util.compareUnsigned(currentKey, key) <= 0) {
          throw new IllegalStateException("Cannot write " + currentKey + " after " + key);
        }
      }
      highLowContainer.append(currentKey, container.runOptimize());
      ++currentKey;
      container = new ArrayContainer();
    }
  }
}