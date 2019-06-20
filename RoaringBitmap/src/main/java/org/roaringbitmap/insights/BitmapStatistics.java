package org.roaringbitmap.insights;

import java.util.Objects;

public class BitmapStatistics {
  private final long bitmapsCount;
  private final ArrayContainersStats arrayContainersStats;
  private final long bitmapContainerCount;
  private final long runContainerCount;

  BitmapStatistics(
      ArrayContainersStats arrayContainersStats,
      long bitmapContainerCount,
      long runContainerCount) {

    this(arrayContainersStats, bitmapContainerCount, runContainerCount, 1);
  }

  BitmapStatistics(
      ArrayContainersStats arrayContainersStats,
      long bitmapContainerCount,
      long runContainerCount,
      long bitmapsCount) {

    this.arrayContainersStats = arrayContainersStats;
    this.bitmapContainerCount = bitmapContainerCount;
    this.runContainerCount = runContainerCount;
    this.bitmapsCount = bitmapsCount;
  }


  /**
   * Calculates what fraction of all containers is the `containerTypeCount`
   * @param containerTypeCount denominator
   * @return some fraction
   */
  public double containerFraction(long containerTypeCount) {
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

  public long containerCount() {
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

  public long getBitmapsCount() {
    return bitmapsCount;
  }

  public long getBitmapContainerCount() {
    return bitmapContainerCount;
  }

  public long getRunContainerCount() {
    return runContainerCount;
  }

  public static class ArrayContainersStats {
    private final long containersCount;

    private final long cardinalitySum;

    public long getContainersCount() {
      return containersCount;
    }

    public long getCardinalitySum() {
      return cardinalitySum;
    }

    ArrayContainersStats(long containersCount, long cardinalitySum) {
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
     * @return the average
     */
    public long averageCardinality() {
      if (containersCount == 0) {
        return Long.MAX_VALUE;
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
