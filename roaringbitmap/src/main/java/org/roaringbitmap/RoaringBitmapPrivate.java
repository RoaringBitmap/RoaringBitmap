package org.roaringbitmap;

/**
 * This class enables accessing/executing not-public methods.
 * Its usage should be reserved to very specific cases, and
 * given should not be considered as part of the official API.
 */
@Deprecated
public class RoaringBitmapPrivate {

  public static void naivelazyor(RoaringBitmap x1, RoaringBitmap x2) {
    x1.naivelazyor(x2);
  }

  public static void repairAfterLazy(RoaringBitmap r) {
    r.repairAfterLazy();
  }
}
