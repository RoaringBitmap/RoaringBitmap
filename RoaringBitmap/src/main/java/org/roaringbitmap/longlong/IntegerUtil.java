package org.roaringbitmap.longlong;

public class IntegerUtil {

  /**
   * convert integer to its byte array format
   */
  public static byte[] toBDBytes(int v) {
    byte[] bytes = new byte[4];
    bytes[0] = (byte) (v >> 24);
    bytes[1] = (byte) (v >> 16);
    bytes[2] = (byte) (v >> 8);
    bytes[3] = (byte) v;
    return bytes;
  }

  /**
   * convert into its integer representation
   */
  public static int fromBDBytes(byte[] bytes) {
    return (bytes[0] & 0xFF) << 24
        | (bytes[1] & 0xFF) << 16
        | (bytes[2] & 0xFF) << 8
        | bytes[3] & 0xFF;
  }

  /**
   * set a specified position byte to another value to return a fresh integer
   */
  public static int setByte(int v, byte bv, int pos) {
    byte[] bytes = toBDBytes(v);
    bytes[pos] = bv;
    return fromBDBytes(bytes);
  }

  /**
   * shift the byte left from the specified position
   */
  public static int shiftLeftFromSpecifiedPosition(int v, int pos, int count) {
    byte[] initialVal = toBDBytes(v);
    System.arraycopy(initialVal, pos + 1, initialVal, pos, count);
    return fromBDBytes(initialVal);
  }

  /**
   * fetch the first byte
   */
  public static byte firstByte(int v) {
    return (byte) (v >> 24);
  }
}
