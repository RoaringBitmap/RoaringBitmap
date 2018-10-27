package org.roaringbitmap.insights;

import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RandomisedTestData;
import org.roaringbitmap.RoaringBitmap;

import static org.junit.Assert.assertEquals;

public class BitmapAnalyserTest {

  @Test
  public void analyseSingleBitmap() {
    RoaringBitmap rb = new RoaringBitmap();
    rb.add(1, 3, 6, 26, 110, 1024);
    rb.add(70000L, 80000L);
    for (int i = (5 << 16); i < (6 << 16); i++) {
      rb.add(i);
    }
    BitmapStatistics result = BitmapAnalyser.analyse(rb);
    BitmapStatistics expected = new BitmapStatistics(new BitmapStatistics.ArrayContainersStats(1, 7), 1, 1);
    assertEquals(expected, result);
  }

  @Test
  public void analyseRandomBitmap() {
    double delta = 0.05;
    double rleLimit = 0.1;
    double denseLimit = 0.2;
    double sparseLimit = 1 - rleLimit - denseLimit;
    RoaringBitmap rb = RandomisedTestData.randomBitmap(1000, rleLimit, denseLimit);
    BitmapStatistics result = BitmapAnalyser.analyse(rb);

    Assert.assertTrue(Math.abs(rleLimit - result.containerFraction(result.runContainerCount)) < delta);
    Assert.assertTrue(Math.abs(denseLimit - result.containerFraction(result.bitmapContainerCount)) < delta);
    Assert.assertTrue(Math.abs(sparseLimit - result.containerFraction(result.arrayContainersStats.containersCount)) < delta);
  }
}
