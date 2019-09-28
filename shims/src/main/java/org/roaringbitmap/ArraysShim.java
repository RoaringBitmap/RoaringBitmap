package org.roaringbitmap;

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
   * Find the index of the first non empty word.
   *
   * @param bitmap the bitmap
   * @return the index of the first non empty word, or -1 if the bitmap is empty
   */
  public static int indexOfFirstNonEmptyWord(long[] bitmap) {
    for (int i = 0; i < bitmap.length; ++i) {
      if (bitmap[i] != 0) {
        return i;
      }
    }
    return -1;
  }
}
