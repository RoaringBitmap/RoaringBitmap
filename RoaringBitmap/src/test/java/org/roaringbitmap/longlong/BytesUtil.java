package org.roaringbitmap.longlong;

public class BytesUtil {

  public static boolean same(byte[] k1, byte[] k2) {
    if (k1.length != k2.length) {
      return false;
    }
    int len = k1.length;
    for (int i = 0; i < len; i++) {
      if (k1[i] != k2[i]) {
        return false;
      }
    }
    return true;
  }
}
