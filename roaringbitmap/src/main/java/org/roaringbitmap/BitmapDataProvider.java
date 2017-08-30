/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

/**
 * Representing a general bitmap interface.
 *
 */
public interface BitmapDataProvider extends ImmutableBitmapDataProvider {
  /**
   * set the value to "true", whether it already appears or not.
   *
   * @param x integer value
   */
  public void add(int x);

  /**
   * If present remove the specified integers (effectively, sets its bit value to false)
   *
   * @param x integer value representing the index in a bitmap
   */
  public void remove(int x);

  /**
   * Recover allocated but unused memory.
   */
  public void trim();
}
