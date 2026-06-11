package org.roaringbitmap;

/**
 * Provides more performant way to fill char array by chars with consecutive codes. It is necessary
 * to call {@link CharRangeFiller#allocate(char)} before use, otherwise simple (and slow) loop is
 * used only. Maximum of used memory is 128 kB, when all characters range is used.
 */
public class CharRangeFiller {
  static final int USE_ARRAYCOPY_MIN_SIZE = 12; // determined empirically via benchmarking
  private static char[] CHARS = new char[0];

  static void fillIteratively(char[] a, int pos, int from, int to) {
    for (int i = 0; i < to - from; i++) {
      a[pos + i] = (char) (from + i);
    }
  }

  /**
   * Allocates char array for complete char range.
   */
  public static void allocateCompletely() {
    allocate(Character.MAX_VALUE);
  }

  /**
   * Allocates char array from zero to up to given character.
   *
   * @param last last allocated character
   */
  public static void allocate(char last) {
    int length = last + 1; // + 1 to have space for null character
    char[] oldChars = CHARS; // grab reference first to be thread-safe
    if (length != oldChars.length) {
      char[] newChars = new char[length];
      int copied = Math.min(length, oldChars.length);
      System.arraycopy(oldChars, 0, newChars, 0, copied);
      fillIteratively(newChars, copied, copied, length);
      CHARS = newChars;
    }
  }

  /**
   * Deallocates char array.
   */
  public static void deallocate() {
    CHARS = new char[0];
  }

  /**
   * Returns length of allocated char array.
   *
   * @return array length
   */
  public static int length() {
    return CHARS.length;
  }

  /**
   * Fills natural numbers to array effectively using pre-allocated char array. If range to fill is
   * too small (several characters), it falls back to simple loop iterating on array as it is more
   * performant for such cases or when pre-allocated array does not contain values from filled
   * range. The length of copied sequence is {@code to - from}. The array could have length greater
   * than char range even it is not expected to happen.
   *
   * @param a    destination char array
   * @param pos  start position in destination array
   * @param from first character code (inclusive)
   * @param to   last character code (exclusive)
   * @throws IndexOutOfBoundsException if @{code pos} is negative, any value of given range is
   *                                   outside char range or filled range does not fit into array
   */
  public static void fill(char[] a, int pos, int from, int to) {
    if (pos < 0) {
      throw new IndexOutOfBoundsException("target position could not be negative: " + pos);
    }
    if (from < 0 || from > Character.MAX_VALUE || to < 0 || to > Character.MAX_VALUE + 1) {
      throw new IndexOutOfBoundsException(
          "values range has to be within char range: (" + from + "," + to + ")");
    }
    if (from >= to) {
      return;
    }
    if (to - from + pos > a.length) {
      throw new IndexOutOfBoundsException(
          "array length "
              + a.length
              + " is too small to fill characters from "
              + from
              + " to "
              + to
              + " from "
              + pos
              + ". position");
    }
    final char[] naturals = CHARS; // grab reference first to be thread-safe
    int copyTo = Math.min(to, naturals.length);
    int iterateFrom;
    int targetFrom;
    if (USE_ARRAYCOPY_MIN_SIZE <= copyTo - from) {
      iterateFrom = copyTo;
      targetFrom = pos + copyTo - from;
      System.arraycopy(naturals, from, a, pos, copyTo - from);
    } else {
      iterateFrom = from;
      targetFrom = pos;
    }
    fillIteratively(a, targetFrom, iterateFrom, to);
  }
}
