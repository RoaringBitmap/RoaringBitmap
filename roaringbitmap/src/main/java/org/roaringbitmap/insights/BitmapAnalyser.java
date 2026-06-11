package org.roaringbitmap.insights;

import org.roaringbitmap.ContainerPointer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RunContainer;

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
    int rcCardinalitySum = 0;
    int bcCount = 0;
    int rcCount = 0;
    int[] runLengthHistogram = new int[16];
    ContainerPointer cp = r.getContainerPointer();
    while (cp.getContainer() != null) {
      if (cp.isBitmapContainer()) {
        bcCount += 1;
      } else if (cp.isRunContainer()) {
        rcCount += 1;
        RunContainer rc = (RunContainer) cp.getContainer();
        int numberOfRuns = rc.numberOfRuns();
        for (int i = 0; i < numberOfRuns; i++) {
          int length = rc.getLength(i);
          runLengthHistogram[Integer.numberOfLeadingZeros(length) - 17]++;
        }

        rcCardinalitySum += cp.getCardinality();
      } else {
        acCount += 1;
        acCardinalitySum += cp.getCardinality();
      }
      cp.advance();
    }
    BitmapStatistics.ArrayContainersStats acStats =
        new BitmapStatistics.ArrayContainersStats(acCount, acCardinalitySum);
    BitmapStatistics.RunContainersStats rcStats =
        new BitmapStatistics.RunContainersStats(rcCount, rcCardinalitySum, runLengthHistogram);
    return new BitmapStatistics(acStats, rcStats, bcCount, rcCount);
  }

  /**
   * Analyze the internal representation of bitmaps
   * @param bitmaps the bitmaps
   * @return the statistics
   */
  public static BitmapStatistics analyse(Collection<? extends RoaringBitmap> bitmaps) {
    return bitmaps.stream()
        .reduce(
            BitmapStatistics.empty,
            (acc, r) -> acc.merge(BitmapAnalyser.analyse(r)),
            BitmapStatistics::merge);
  }
}
