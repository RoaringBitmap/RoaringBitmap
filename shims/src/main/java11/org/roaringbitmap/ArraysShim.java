package org.roaringbitmap;

import java.util.Arrays;

/**
 * Shim over JDK11 methods in Arrays to support multi-release
 */
public class ArraysShim {

  /**
   * Checks if the two arrays are equal within the given range.
   * @param x the first array
   * @param xmin the inclusive minimum of the range of the first array
   * @param xmax the exclusive maximum of the range of the first array
   * @param y the second array
   * @param ymin the inclusive minimum of the range of the second array
   * @param ymax the exclusive maximum of the range of the second array
   * @return true if the arrays are equal in the specified ranges
   */
  public static boolean equals(short[] x, int xmin, int xmax, short[] y, int ymin, int ymax) {
    return Arrays.equals(x, xmin, xmax, y, ymin, ymax);
  }
}
