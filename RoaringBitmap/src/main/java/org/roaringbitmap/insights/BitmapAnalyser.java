package org.roaringbitmap.insights;

import org.roaringbitmap.ContainerPointer;
import org.roaringbitmap.RoaringBitmap;

import java.util.Collection;

public class BitmapAnalyser {

  /**
   * Analyze the internal representation of bitmap
   * @param r the bitmap
   * @return the statistics
   */
  public static BitmapStatistics analyse(RoaringBitmap r) {
    int acCount = 0;
    int acCardinalitySum = 0;
    int bcCount = 0;
    int rcCount = 0;
    ContainerPointer cp = r.getContainerPointer();
    while (cp.getContainer() != null) {
      if (cp.isBitmapContainer()) {
        bcCount += 1;
      } else if (cp.isRunContainer()) {
        rcCount += 1;
      } else {
        acCount += 1;
        acCardinalitySum += cp.getCardinality();
      }
      cp.advance();
    }
    BitmapStatistics.ArrayContainersStats acStats =
        new BitmapStatistics.ArrayContainersStats(acCount, acCardinalitySum);
    return new BitmapStatistics(acStats, bcCount, rcCount);
  }

  /**
   * Analyze the internal representation of bitmaps
   * @param bitmaps the bitmaps
   * @return the statistics
   */
  public static BitmapStatistics analyse(Collection<? extends RoaringBitmap> bitmaps) {
    return bitmaps
      .stream()
      .reduce(
        BitmapStatistics.empty,
        (acc, r) -> acc.merge(BitmapAnalyser.analyse(r)),
        BitmapStatistics::merge);
  }
}
