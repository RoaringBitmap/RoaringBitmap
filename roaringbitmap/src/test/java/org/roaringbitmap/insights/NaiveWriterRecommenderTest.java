package org.roaringbitmap.insights;

import org.junit.Assert;
import org.junit.Test;

public class NaiveWriterRecommenderTest {

  @Test
  public void recommendForArrays() {
    int arrayContainerCount = 20000;
    int averagePerArrayContaier = 10;
    int bitmapContainerCount = 2;
    int runContainerCount = 50;
    int bitmapsCount = 10;
    BitmapStatistics stats = new BitmapStatistics(
      new BitmapStatistics.ArrayContainersStats(arrayContainerCount, arrayContainerCount * averagePerArrayContaier),
      bitmapContainerCount,
      runContainerCount,
      bitmapsCount);

    String recommendation = NaiveWriterRecommender.recommend(stats);

    //System.out.println(recommendation);
    Assert.assertTrue(recommendation.contains(".initialCapacity(2005)"));
    Assert.assertTrue(recommendation.contains(".optimiseForArrays()"));
    Assert.assertTrue(recommendation.contains(".expectedContainerSize(10)"));
  }

  @Test
  public void recommendForRuns() {
    int arrayContainerCount = 100;
    int averagePerArrayContaier = 10;
    int bitmapContainerCount = 200;
    int runContainerCount = 50000;
    int bitmapsCount = 70;
    BitmapStatistics stats = new BitmapStatistics(
      new BitmapStatistics.ArrayContainersStats(arrayContainerCount, arrayContainerCount * averagePerArrayContaier),
      bitmapContainerCount,
      runContainerCount,
      bitmapsCount);

    String recommendation = NaiveWriterRecommender.recommend(stats);

    Assert.assertTrue(recommendation.contains(".initialCapacity(718)"));
    Assert.assertTrue(recommendation.contains(".optimiseForRuns()"));
  }


  @Test
  public void recommendForUniform() {
    int arrayContainerCount = 10000;
    int averagePerArrayContaier = 10;
    int bitmapContainerCount = 10000;
    int runContainerCount = 10000;
    int bitmapsCount = 120;
    BitmapStatistics stats = new BitmapStatistics(
      new BitmapStatistics.ArrayContainersStats(arrayContainerCount, arrayContainerCount * averagePerArrayContaier),
      bitmapContainerCount,
      runContainerCount,
      bitmapsCount);

    String recommendation = NaiveWriterRecommender.recommend(stats);

    Assert.assertTrue(recommendation.contains(".initialCapacity(250)"));
    Assert.assertTrue(recommendation.contains(".constantMemory()"));
  }

  @Test
  public void recommendForBitmaps() {
    int arrayContainerCount = 7;
    int averagePerArrayContaier = 10;
    int bitmapContainerCount = 100000;
    int runContainerCount = 40;
    int bitmapsCount = 190;
    BitmapStatistics stats = new BitmapStatistics(
      new BitmapStatistics.ArrayContainersStats(arrayContainerCount, arrayContainerCount * averagePerArrayContaier),
      bitmapContainerCount,
      runContainerCount,
      bitmapsCount);

    String recommendation = NaiveWriterRecommender.recommend(stats);

    Assert.assertTrue(recommendation.contains(".initialCapacity(526)"));
    Assert.assertTrue(recommendation.contains(".constantMemory()"));
  }

}
