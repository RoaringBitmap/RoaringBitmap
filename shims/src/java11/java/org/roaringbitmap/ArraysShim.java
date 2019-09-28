package org.roaringbitmap;

import java.util.Arrays;

/**
 * Shim over JDK11 methods in Arrays to support multi-release
 */
public class ArraysShim {

  private static final long[] EMPTY = new long[64];

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

  /**
   * Find the index of the first non empty word
   *
   * @param bitmap the bitmap
   * @return the index of the first non empty word, or -1 if the bitmap is empty
   */
  public static int indexOfFirstNonEmptyWord(long[] bitmap) {
    int firstNonEmpty = -1;
    for (int i = 0; i < bitmap.length && firstNonEmpty == -1; i += EMPTY.length) {
      firstNonEmpty = Arrays.mismatch(bitmap, i, i + EMPTY.length, EMPTY, 0, EMPTY.length);
    }
    return firstNonEmpty;
  }
}
