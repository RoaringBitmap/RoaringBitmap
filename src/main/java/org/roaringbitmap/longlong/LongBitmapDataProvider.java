/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap.longlong;

/**
 * Representing a general bitmap interface.
 *
 */
public interface LongBitmapDataProvider extends ImmutableLongBitmapDataProvider {
  /**
   * set the value to "true", whether it already appears or not.
   *
   * @param x long value
   */
  public void addLong(long x);

  /**
   * If present remove the specified integers (effectively, sets its bit value to false)
   *
   * @param x long value representing the index in a bitmap
   */
  public void removeLong(long x);


  /**
   * Recover allocated but unused memory.
   */
  public void trim();
}
