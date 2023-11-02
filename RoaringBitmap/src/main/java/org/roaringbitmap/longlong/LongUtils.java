package org.roaringbitmap.longlong;

public class LongUtils {

  public static final long MAX_UNSIGNED_INT = Integer.toUnsignedLong(0xFFFFFFFF);

  /**
   * get the high 48 bit parts of the input data
   *
   * @param num the long number
   * @return the high 48 bit
   */
  public static byte[] highPart(long num) {
    return new byte[]{
        (byte) ((num >>> 56) & 0xff),
        (byte) ((num >>> 48) & 0xff),
        (byte) ((num >>> 40) & 0xff),
        (byte) ((num >>> 32) & 0xff),
        (byte) ((num >>> 24) & 0xff),
        (byte) ((num >>> 16) & 0xff)
    };
  }

  /**
   * get the low 16 bit parts of the input data
   *
   * @param num the long number
   * @return the low 16 bit
   */
  public static char lowPart(long num) {
    return (char) num;
  }

  /**
   * reconstruct the long data
   *
   * @param high the high 48 bit
   * @param low the low 16 bit
   * @return the long data
   */
  public static long toLong(byte[] high, char low) {
    return toLong(high) << 16 | low;
  }

  /**
   * Reconstruct the long data.
   *
   * @param high the high 48 bit
   * @return the long data
   */
  public static long toLong(byte[] high) {
    return (high[0] & 0xFFL) << 40
        | (high[1] & 0xFFL) << 32
        | (high[2] & 0xFFL) << 24
        | (high[3] & 0xFFL) << 16
        | (high[4] & 0xFFL) << 8
        | (high[5] & 0xFFL);
  }

  public static long toLong(long high, char low) {
    return high << 16 | low;
  }

  /**
   * to big endian bytes representation
   * @param v a long value
   * @return the input long value's big endian byte array representation
   */
  public static byte[] toBDBytes(long v) {
    byte[] work = new byte[8];
    work[7] = (byte) v;
    work[6] = (byte) (v >> 8);
    work[5] = (byte) (v >> 16);
    work[4] = (byte) (v >> 24);
    work[3] = (byte) (v >> 32);
    work[2] = (byte) (v >> 40);
    work[1] = (byte) (v >> 48);
    work[0] = (byte) (v >> 56);
    return work;
  }

  /**
   * get the long from the big endian representation bytes
   *
   * @param work the byte array
   * @return the long data
   */
  public static long fromBDBytes(byte[] work) {
    return (long) (work[0]) << 56
        /* long cast needed or shift done modulo 32 */
        | (long) (work[1] & 0xff) << 48
        | (long) (work[2] & 0xff) << 40
        | (long) (work[3] & 0xff) << 32
        | (long) (work[4] & 0xff) << 24
        | (long) (work[5] & 0xff) << 16
        | (long) (work[6] & 0xff) << 8
        | (long) (work[7] & 0xff);
  }

  /**
   * initialize a long value with the given fist 32 bit
   *
   * @param v first 32 bit value
   * @return a long value
   */
  public static long initWithFirst4Byte(int v) {
    return ((long) v) << 32;
  }

  /**
   * shift the long right by the container size amount so we can loop across containers by +1 steps
   * @param num long being treated as unsigned long
   * @return value shifted out of value space into container high part
   */
  public static long rightShiftHighPart(long num) {
    return num >>> 16;
  }

  /**
   * shift the long by left the container size amount so we use the value after have done our steps
   * @param num uint48 to be shift back into uint64
   * @return value shifted out of container high part back into value space
   */
  public static long leftShiftHighPart(long num) {
    return num << 16;
  }

  public static int maxLowBitAsInteger() {
    return 0xFFFF;
  }

  /**
   * set the high 48 bit parts of the input number into the given byte array
   *
   * @param num the long number
   * @param high48 the byte array
   * @return the high 48 bit
   */
  public static byte[] highPartInPlace(long num, byte[] high48) {
    high48[0] = (byte) ((num >>> 56) & 0xff);
    high48[1] = (byte) ((num >>> 48) & 0xff);
    high48[2] = (byte) ((num >>> 40) & 0xff);
    high48[3] = (byte) ((num >>> 32) & 0xff);
    high48[4] = (byte) ((num >>> 24) & 0xff);
    high48[5] = (byte) ((num >>> 16) & 0xff);
    return high48;
  }

  /**
   * checks if given high48 is the maximum possible one
   * (e.g. it is the case for -1L, which is the maximum unsigned long)
   *
   * @param key long
   * @return true if this the maximum high part
   */
  public static boolean isMaxHigh(long key) {
    return (key & 0xFF_FF_FF_FF_FF_FFL) == 0xFF_FF_FF_FF_FF_FFL;
  }
}
