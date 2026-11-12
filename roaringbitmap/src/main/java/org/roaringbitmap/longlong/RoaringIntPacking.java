/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.longlong;

import java.util.Comparator;

/**
 * Used to hold the logic packing 2 integers in a long, and separating a long in two integers. It is
 * useful in {@link Roaring64NavigableMap} as the implementation split the input long in two
 * integers, one used as key of a NavigableMap while the other is added in a Bitmap
 *
 * @author Benoit Lacelle
 *
 */
class RoaringIntPacking {

  /**
   *
   * @param id any long, positive or negative
   * @return an int holding the 32 highest order bits of information of the input long
   */
  public static int high(long id) {
    return (int) (id >> 32);
  }

  /**
   *
   * @param id any long, positive or negative
   * @return an int holding the 32 lowest order bits of information of the input long
   */
  public static int low(long id) {
    return (int) id;
  }

  /**
   *
   * @param high an integer representing the highest order bits of the output long
   * @param low an integer representing the lowest order bits of the output long
   * @return a long packing together the integers as computed by
   *         {@link RoaringIntPacking#high(long)} and {@link RoaringIntPacking#low(long)}
   */
  // https://stackoverflow.com/questions/12772939/java-storing-two-ints-in-a-long
  public static long pack(int high, int low) {
    return (((long) high) << 32) | (low & 0xffffffffL);
  }

  /**
   *
   * @param signedLongs true if long put in a {@link Roaring64NavigableMap} should be considered as
   *        signed long.
   * @return the int representing the highest value which can be set as high value in a
   *         {@link Roaring64NavigableMap}
   */
  public static int highestHigh(boolean signedLongs) {
    if (signedLongs) {
      return Integer.MAX_VALUE;
    } else {
      return -1;
    }
  }

  /**
   * @return A comparator for unsigned longs: a negative long is a long greater than Long.MAX_VALUE
   */
  public static Comparator<Integer> unsignedComparator() {
    return Integer::compareUnsigned;
  }

  /**
   * Equivalent to {@link Integer#compareUnsigned(int, int)}.
   *
   * @param x the first {@code int} to compare
   * @param y the second {@code int} to compare
   * @return the result of comparing {@code x} and {@code y} as unsigned values
   */
  public static int compareUnsigned(int x, int y) {
    return Integer.compareUnsigned(x, y);
  }

  /**
   * Equivalent to {@link Long#toUnsignedString(long)}.
   *
   * @param l the value to convert, interpreted as unsigned
   * @return the unsigned decimal representation of {@code l}
   */
  static String toUnsignedString(long l) {
    return Long.toUnsignedString(l);
  }
}
