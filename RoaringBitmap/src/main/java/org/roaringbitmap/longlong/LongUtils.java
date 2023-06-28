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
    byte[] high48 = new byte[]{
        (byte) ((num >>> 56) & 0xff),
        (byte) ((num >>> 48) & 0xff),
        (byte) ((num >>> 40) & 0xff),
        (byte) ((num >>> 32) & 0xff),
        (byte) ((num >>> 24) & 0xff),
        (byte) ((num >>> 16) & 0xff)
    };
    return high48;
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
    byte byte6 = (byte) (low >>> 8 & 0xFFL);
    byte byte7 = (byte) low;
    return (high[0] & 0xFFL) << 56
        | (high[1] & 0xFFL) << 48
        | (high[2] & 0xFFL) << 40
        | (high[3] & 0xFFL) << 32
        | (high[4] & 0xFFL) << 24
        | (high[5] & 0xFFL) << 16
        | (byte6 & 0xFFL) << 8
        | (byte7 & 0xFFL);
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
   * compare according to the dictionary order
   *
   * @param a a byte array
   * @param b another byte array
   * @return positive indicates a greater than b,0 indicates equal,negative indicates a smaller than b
   */
  public static int compareHigh(byte[] a, byte[] b) {
    if (a == b) { return 0; }
    if (a[0] != b[0]) { return Byte.toUnsignedInt(a[0]) - Byte.toUnsignedInt(b[0]); }
    if (a[1] != b[1]) { return Byte.toUnsignedInt(a[1]) - Byte.toUnsignedInt(b[1]); }
    if (a[2] != b[2]) { return Byte.toUnsignedInt(a[2]) - Byte.toUnsignedInt(b[2]); }
    if (a[3] != b[3]) { return Byte.toUnsignedInt(a[3]) - Byte.toUnsignedInt(b[3]); }
    if (a[4] != b[4]) { return Byte.toUnsignedInt(a[4]) - Byte.toUnsignedInt(b[4]); }
    if (a[5] != b[5]) { return Byte.toUnsignedInt(a[5]) - Byte.toUnsignedInt(b[5]); }
    return 0;
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
   * @param high48 the byte array
   * @return true if this the maximum high part
   */
  public static boolean isMaxHigh(byte[] high48) {
    return high48[0] == -1
            && high48[1] == -1
            && high48[2] == -1
            && high48[3] == -1
            && high48[4] == -1
            && high48[5] == -1;
  }
}
