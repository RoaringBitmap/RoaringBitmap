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
  void add(int x);

  /**
   * Add a range of values to the bitmap
   * @param min the inclusive minimum value
   * @param sup the exclusive maximum value
   */
  void add(long min, long sup);

  /**
   * If present remove the specified integers (effectively, sets its bit value to false)
   *
   * @param x integer value representing the index in a bitmap
   */
  void remove(int x);

  /**
   * Recover allocated but unused memory.
   */
  void trim();
}
