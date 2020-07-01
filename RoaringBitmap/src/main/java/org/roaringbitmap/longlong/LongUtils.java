package org.roaringbitmap.longlong;

public class LongUtils {

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
   * @return 1 indicates a greater than b,0 indicates equal,-1 indicates a smaller than b
   */
  public static int compareHigh(byte[] a, byte[] b) {
    return compareTo(a, 0, a.length, b, 0, b.length);
  }

  private static int compareTo(byte[] buffer1, int offset1, int length1,
      byte[] buffer2, int offset2, int length2) {
    if (buffer1 == buffer2
        && offset1 == offset2
        && length1 == length2) {
      return 0;
    }
    int end1 = offset1 + length1;
    int end2 = offset2 + length2;
    for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
      int a = (buffer1[i] & 0xff);
      int b = (buffer2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
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
}
