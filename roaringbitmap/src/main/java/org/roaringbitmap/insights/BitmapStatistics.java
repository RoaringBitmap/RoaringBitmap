package org.roaringbitmap.insights;

import java.util.Objects;

public class BitmapStatistics {
  private final int bitmapsCount;
  private final ArrayContainersStats arrayContainersStats;
  private final int bitmapContainerCount;
  private final int runContainerCount;

  BitmapStatistics(
      ArrayContainersStats arrayContainersStats,
      int bitmapContainerCount,
      int runContainerCount) {

    this(arrayContainersStats, bitmapContainerCount, runContainerCount, 1);
  }

  BitmapStatistics(
      ArrayContainersStats arrayContainersStats,
      int bitmapContainerCount,
      int runContainerCount,
      int bitmapsCount) {

    this.arrayContainersStats = arrayContainersStats;
    this.bitmapContainerCount = bitmapContainerCount;
    this.runContainerCount = runContainerCount;
    this.bitmapsCount = bitmapsCount;
  }


  /**
   * Calculates what fraction of all containers is the `containerTypeCount`
   */
  public double containerFraction(int containerTypeCount) {
    if (containerCount() == 0) {
      return Double.NaN;
    } else {
      return ((double) containerTypeCount) / containerCount();
    }
  }

  public ArrayContainersStats getArrayContainersStats() {
    return arrayContainersStats;
  }

  @Override
  public String toString() {
    return "BitmapStatistics{"
      + "bitmapsCount=" + bitmapsCount
      + ", arrayContainersStats=" + arrayContainersStats
      + ", bitmapContainerCount=" + bitmapContainerCount
      + ", runContainerCount=" + runContainerCount
      + '}';
  }

  public int containerCount() {
    return arrayContainersStats.containersCount + bitmapContainerCount + runContainerCount;
  }

  BitmapStatistics merge(BitmapStatistics other) {
    return new BitmapStatistics(
      arrayContainersStats.merge(other.arrayContainersStats),
      bitmapContainerCount + other.bitmapContainerCount,
      runContainerCount + other.runContainerCount,
      bitmapsCount + other.bitmapsCount);
  }

  public final static BitmapStatistics empty = new BitmapStatistics(
      ArrayContainersStats.empty, 0, 0, 0);

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BitmapStatistics that = (BitmapStatistics) o;
    return bitmapsCount == that.bitmapsCount
      && bitmapContainerCount == that.bitmapContainerCount
      && runContainerCount == that.runContainerCount
      && Objects.equals(arrayContainersStats, that.arrayContainersStats);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bitmapsCount, bitmapContainerCount, runContainerCount);
  }

  public int getBitmapsCount() {
    return bitmapsCount;
  }

  public int getBitmapContainerCount() {
    return bitmapContainerCount;
  }

  public int getRunContainerCount() {
    return runContainerCount;
  }

  public static class ArrayContainersStats {
    private final int containersCount;

    public int getContainersCount() {
      return containersCount;
    }

    public int getCardinalitySum() {
      return cardinalitySum;
    }

    private final int cardinalitySum;


    ArrayContainersStats(int containersCount, int cardinalitySum) {
      this.containersCount = containersCount;
      this.cardinalitySum = cardinalitySum;
    }

    ArrayContainersStats merge(ArrayContainersStats other) {
      return new ArrayContainersStats(
        containersCount + other.containersCount,
        cardinalitySum + other.cardinalitySum);
    }

    /**
     * Average cardinality of ArrayContainers
     */
    public int averageCardinality() {
      if (containersCount == 0) {
        return Integer.MAX_VALUE;
      } else {
        return cardinalitySum / containersCount;
      }
    }


    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ArrayContainersStats that = (ArrayContainersStats) o;
      return containersCount == that.containersCount
        && cardinalitySum == that.cardinalitySum;
    }

    @Override
    public int hashCode() {
      return Objects.hash(containersCount, cardinalitySum);
    }

    @Override
    public String toString() {
      return "ArrayContainersStats{"
        + "containersCount=" + containersCount
        + ", cardinalitySum=" + cardinalitySum
        + '}';
    }

    public final static ArrayContainersStats empty = new ArrayContainersStats(0, 0);
  }


}
