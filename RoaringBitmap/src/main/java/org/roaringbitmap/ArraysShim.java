package org.roaringbitmap;

/**
 * Shim over JDK11 methods in Arrays to support multi-release
 */
public class ArraysShim {

  /**
   * Checks if the two arrays are equal within the given range.
   *
   * @param x the first array
   * @param xmin the inclusive minimum of the range of the first array
   * @param xmax the exclusive maximum of the range of the first array
   * @param y the second array
   * @param ymin the inclusive minimum of the range of the second array
   * @param ymax the exclusive maximum of the range of the second array
   * @return true if the arrays are equal in the specified ranges
   */
  public static boolean equals(char[] x, int xmin, int xmax, char[] y, int ymin, int ymax) {
    int xlen = xmax - xmin;
    int ylen = ymax - ymin;
    if (xlen != ylen) {
      return false;
    }
    for (int i = xmin, j = ymin; i < xmax && j < ymax; ++i, ++j) {
      if (x[i] != y[j]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Finds and returns the relative index of the first mismatch between two byte arrays over the
   * specified ranges,otherwise return -1 if no mismatch is found. The index will be in the range of
   * 0 (inclusive) up to the length (inclusive) of the smaller range.
   *
   * @param a one input byte array
   * @param aFromIndex inclusive
   * @param aToIndex exclusive
   * @param b another input byte array
   * @param bFromIndex inclusive
   * @param bToIndex exclusive
   * @return -1 if no mismatch found,otherwise the mismatch offset
   */
  public static int mismatch(byte[] a, int aFromIndex, int aToIndex,
      byte[] b, int bFromIndex, int bToIndex) {
    int aLength = aToIndex - aFromIndex;
    int bLength = bToIndex - bFromIndex;
    int length = Math.min(aLength, bLength);
    for (int i = 0; i < length; i++) {
      if (a[aFromIndex + i] != b[bFromIndex + i]) {
        return i;
      }
    }
    if (aLength != bLength) {
      return length;
    }
    return -1;
  }
}
