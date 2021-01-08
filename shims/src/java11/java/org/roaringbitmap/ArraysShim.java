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
  public static boolean equals(char[] x, int xmin, int xmax, char[] y, int ymin, int ymax) {
    return Arrays.equals(x, xmin, xmax, y, ymin, ymax);
  }

  /**
   * Finds and returns the relative index of the first mismatch between two byte arrays over the
   * specified ranges,otherwise return -1 if no mismatch is found. The index will be in the range of
   * 0 (inclusive) up to the length (inclusive) of the smaller range.
   *
   * @param a a input byte array
   * @param aFromIndex inclusive
   * @param aToIndex exclusive
   * @param b another input byte array
   * @param bFromIndex inclusive
   * @param bToIndex exclusive
   * @return -1 if no mismatch found,othewise the mismatch offset
   */
  public static int mismatch(byte[] a, int aFromIndex, int aToIndex,
      byte[] b, int bFromIndex, int bToIndex) {
    if (bFromIndex > bToIndex) {
      return -1;
    }
    return Arrays.mismatch(a, aFromIndex, aToIndex, b, bFromIndex, bToIndex);
  }
}
