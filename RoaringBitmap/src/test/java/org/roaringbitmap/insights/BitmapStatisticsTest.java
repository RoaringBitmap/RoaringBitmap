package org.roaringbitmap.insights;

import org.junit.Assert;
import org.junit.Test;

public class BitmapStatisticsTest {

  @Test
  public void toStringWorks() {
    BitmapStatistics statistics = new BitmapStatistics(
      new BitmapStatistics.ArrayContainersStats(10, 50),
      2,
      1);

    String string = statistics.toString();

    Assert.assertTrue(string.contains(BitmapStatistics.class.getSimpleName()));
  }

  @Test
  public void statsForEmpty() {
    BitmapStatistics statistics = BitmapStatistics.empty;

    double bitmapFraction = statistics.containerFraction(statistics.getBitmapContainerCount());
    Assert.assertTrue(Double.isNaN(bitmapFraction));
    long averageArraysCardinality = statistics.getArrayContainersStats().averageCardinality();
    Assert.assertEquals(Long.MAX_VALUE, averageArraysCardinality);
  }
}
