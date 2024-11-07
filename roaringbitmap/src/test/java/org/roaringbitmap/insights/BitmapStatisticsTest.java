package org.roaringbitmap.insights;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class BitmapStatisticsTest {

  @Test
  public void toStringWorks() {
    BitmapStatistics statistics =
        new BitmapStatistics(new BitmapStatistics.ArrayContainersStats(10, 50), 2, 1);

    String string = statistics.toString();

    assertTrue(string.contains(BitmapStatistics.class.getSimpleName()));
  }

  @Test
  public void statsForEmpty() {
    BitmapStatistics statistics = BitmapStatistics.empty;

    double bitmapFraction = statistics.containerFraction(statistics.getBitmapContainerCount());
    assertTrue(Double.isNaN(bitmapFraction));
    long averageArraysCardinality = statistics.getArrayContainersStats().averageCardinality();
    assertEquals(Long.MAX_VALUE, averageArraysCardinality);
  }
}
