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
// Hidden as it holds implementation details for RoaringTreeMap. We may decide to change the logic
// here, hence it should
// not be used elsewhere
class RoaringIntPacking {

  /**
   * 
   * @param id a long to decompose into two integers
   * @return an int holding 32 bits of information of a long. Typically the highest order bits.
   */
  // TODO: enable an int with the expected qualities while considering the input long as a 64bits
  // unsigned long
  public static int high(long id) {
    return (int) (id >> 32);
  }

  /**
   * 
   * @param id a long to decompose into two integers
   * @return an int holding 32 bits of information of a long. Typically the lowest order bits.
   */
  // TODO: enable an int with the expected qualities while considering the input long as a 64bits
  // unsigned long
  public static int low(long id) {
    return (int) id;
  }

  // https://stackoverflow.com/questions/12772939/java-storing-two-ints-in-a-long
  public static long pack(int high, int low) {
    return (((long) high) << 32) | (low & 0xffffffffL);
  }



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

  // Duplicated from jdk8 Integer.toUnsignedLong
  static long toUnsignedLong(int x) {
    return ((long) x) & 0xffffffffL;
  }
}
