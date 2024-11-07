package org.roaringbitmap.insights;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.SeededTestData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.List;

@Execution(ExecutionMode.CONCURRENT)
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
    BitmapStatistics expected =
        new BitmapStatistics(new BitmapStatistics.ArrayContainersStats(1, 6), 1, 1);
    assertEquals(expected, result);
  }

  @Test
  public void analyseRandomBitmap() {
    double delta = 0.1;
    double runFraction = 0.1;
    double bitmapFraction = 0.2;
    double denseLimit = runFraction + bitmapFraction;
    double arrayFraction = 1 - denseLimit;
    RoaringBitmap rb = SeededTestData.randomBitmap(1000, runFraction, denseLimit);
    BitmapStatistics result = BitmapAnalyser.analyse(rb);

    assertEquals(runFraction, result.containerFraction(result.getRunContainerCount()), delta);
    assertEquals(bitmapFraction, result.containerFraction(result.getBitmapContainerCount()), delta);
    assertEquals(
        arrayFraction,
        result.containerFraction(result.getArrayContainersStats().getContainersCount()),
        delta);
  }

  @Test
  public void analyseRandomBitmaps() {
    double delta = 0.1;
    double runFraction = 0.05;
    double bitmapFraction = 0.5;
    double denseLimit = runFraction + bitmapFraction;
    double arrayFraction = 1 - denseLimit;
    List<RoaringBitmap> bitmaps = new ArrayList<>();
    int totalBitmaps = 60;
    for (int i = 0; i < totalBitmaps; i++) {
      RoaringBitmap rb = SeededTestData.randomBitmap(80, runFraction, denseLimit);
      bitmaps.add(rb);
    }

    BitmapStatistics result = BitmapAnalyser.analyse(bitmaps);

    assertEquals(runFraction, result.containerFraction(result.getRunContainerCount()), delta);
    assertEquals(bitmapFraction, result.containerFraction(result.getBitmapContainerCount()), delta);
    assertEquals(
        arrayFraction,
        result.containerFraction(result.getArrayContainersStats().getContainersCount()),
        delta);
    assertEquals(totalBitmaps, result.getBitmapsCount());
  }
}
