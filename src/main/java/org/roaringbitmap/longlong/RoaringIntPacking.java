/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.roaringbitmap.longlong;

import java.math.BigInteger;
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
    return new Comparator<Integer>() {

      @Override
      public int compare(Integer o1, Integer o2) {
        return compareUnsigned(o1, o2);
      }
    };
  }

  /**
   * Compares two {@code int} values numerically treating the values as unsigned.
   *
   * @param x the first {@code int} to compare
   * @param y the second {@code int} to compare
   * @return the value {@code 0} if {@code x == y}; a value less than {@code 0} if {@code x < y} as
   *         unsigned values; and a value greater than {@code 0} if {@code x > y} as unsigned values
   * @since 1.8
   */
  // Duplicated from jdk8 Integer.compareUnsigned
  public static int compareUnsigned(int x, int y) {
    return Integer.compare(x + Integer.MIN_VALUE, y + Integer.MIN_VALUE);
  }

  /** the constant 2^64 */
  private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);

  /**
   * JDK8 Long.toUnsignedString was too complex to backport. Go for a slow version relying on
   * BigInteger
   */
  // https://stackoverflow.com/questions/7031198/java-signed-long-to-unsigned-long-string
  static String toUnsignedString(long l) {
    BigInteger b = BigInteger.valueOf(l);
    if (b.signum() < 0) {
      b = b.add(TWO_64);
    }
    return b.toString();
  }
}
