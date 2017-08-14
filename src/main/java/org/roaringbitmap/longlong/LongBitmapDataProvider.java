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
  // TODO  interesting to keep the int version, to prevent packing and Map indirection for ints
  public void add(long x);

  /**
   * If present remove the specified integers (effectively, sets its bit value to false)
   *
   * @param x long value representing the index in a bitmap
   */
  public void remove(long x);

  /**
   * Return the jth value stored in this bitmap.
   *
   * @param j index of the value
   *
   * @return the value
   */
  @Override
  public long select(long j);

  /**
   * Recover allocated but unused memory.
   */
  public void trim();
}
