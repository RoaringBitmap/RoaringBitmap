package org.roaringbitmap.buffer;

/**
 * This class enables accessing/executing not-public methods.
 * Its usage should be reserved to very specific cases, and
 * given should not be considered as part of the official API.
 */
@Deprecated
public class MutableRoaringBitmapPrivate {

  public static void naivelazyor(MutableRoaringBitmap x1, MutableRoaringBitmap x2) {
    x1.naivelazyor(x2);
  }

  public static void repairAfterLazy(MutableRoaringBitmap r) {
    r.repairAfterLazy();
  }
}
