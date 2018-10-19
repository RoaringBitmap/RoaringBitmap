package org.roaringbitmap;

public interface OrderedWriter {

  /**
   * Gets the bitmap being written to.
   * @return the bitmpa
   */
  RoaringBitmap getUnderlying();

  /**
   * buffers a value to be added to the bitmap.
   * @param value the value
   */
  void add(int value);

  /**
   * Flushes all pending changes to the bitmap.
   */
  void flush();
}
