package org.roaringbitmap.insights;

import java.util.Arrays;
import java.util.Objects;

public class BitmapStatistics {
  private final long bitmapsCount;
  private final ArrayContainersStats arrayContainersStats;
  private final RunContainersStats runContainersStats;
  private final long bitmapContainerCount;
  private final long runContainerCount;

  BitmapStatistics(
      ArrayContainersStats arrayContainersStats,
      RunContainersStats runContainersStats,
      long bitmapContainerCount,
      long runContainerCount) {

    this(arrayContainersStats, runContainersStats, bitmapContainerCount, runContainerCount, 1);
  }

  BitmapStatistics(
      ArrayContainersStats arrayContainersStats,
      RunContainersStats runContainersStats,
      long bitmapContainerCount,
      long runContainerCount,
      long bitmapsCount) {

    this.arrayContainersStats = arrayContainersStats;
    this.runContainersStats = runContainersStats;
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

  public RunContainersStats getRunContainersStats() {
    return runContainersStats;
  }

  @Override
  public String toString() {
    return "BitmapStatistics{"
        + "bitmapsCount="
        + bitmapsCount
        + ", arrayContainersStats="
        + arrayContainersStats
        + ", runContainersStats="
        + runContainersStats
        + ", bitmapContainerCount="
        + bitmapContainerCount
        + ", runContainerCount="
        + runContainerCount
        + '}';
  }

  public long containerCount() {
    return arrayContainersStats.containersCount + bitmapContainerCount + runContainerCount;
  }

  BitmapStatistics merge(BitmapStatistics other) {
    return new BitmapStatistics(
        arrayContainersStats.merge(other.arrayContainersStats),
        runContainersStats.merge(other.runContainersStats),
        bitmapContainerCount + other.bitmapContainerCount,
        runContainerCount + other.runContainerCount,
        bitmapsCount + other.bitmapsCount);
  }

  public static final BitmapStatistics empty =
      new BitmapStatistics(ArrayContainersStats.empty, RunContainersStats.empty, 0, 0, 0);

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
        && Objects.equals(arrayContainersStats, that.arrayContainersStats)
        && Objects.equals(runContainersStats, that.runContainersStats);
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
          containersCount + other.containersCount, cardinalitySum + other.cardinalitySum);
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
      return containersCount == that.containersCount && cardinalitySum == that.cardinalitySum;
    }

    @Override
    public int hashCode() {
      return Objects.hash(containersCount, cardinalitySum);
    }

    @Override
    public String toString() {
      return "ArrayContainersStats{"
          + "containersCount="
          + containersCount
          + ", cardinalitySum="
          + cardinalitySum
          + '}';
    }

    public static final ArrayContainersStats empty = new ArrayContainersStats(0, 0);
  }

  public static class RunContainersStats {
    private final long runsCount;
    private final long cardinalitySum;
    private final int[] runLengthHistogram; // length is always 16

    public long getRunsCount() {
      return runsCount;
    }

    public long getCardinalitySum() {
      return cardinalitySum;
    }

    public int[] getRunLengthHistogram() {
      return runLengthHistogram;
    }

    RunContainersStats(long runsCount, long cardinalitySum, int[] runLengthHistogram) {
      this.runsCount = runsCount;
      this.cardinalitySum = cardinalitySum;
      this.runLengthHistogram = runLengthHistogram;
    }

    RunContainersStats merge(RunContainersStats other) {
      return new RunContainersStats(
          runsCount + other.runsCount,
          cardinalitySum + other.cardinalitySum,
          addArrays(runLengthHistogram, other.runLengthHistogram));
    }

    private static int[] addArrays(int[] a, int[] b) {
      int[] c = new int[a.length];
      for (int i = 0; i < a.length; i++) {
        c[i] = a[i] + b[i];
      }
      return c;
    }

    /**
     * Average run length
     *
     * @return the average
     */
    public long averageRunLength() {
      if (runsCount == 0) {
        return Long.MAX_VALUE;
      } else {
        return cardinalitySum / runsCount;
      }
    }

    // TODO very disputable - at least due to use magic constant 70, above that averageRunLength
    // should be involved also
    public boolean hasManyLongRuns() {
      int sum = 0;
      for (int i = 0; i < 12; i++) {
        sum += runLengthHistogram[i] * (12 - i); // linear weighting, not by powers of two
      }
      return sum > 70;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RunContainersStats that = (RunContainersStats) o;
      return runsCount == that.runsCount
          && cardinalitySum == that.cardinalitySum
          && Arrays.equals(runLengthHistogram, that.runLengthHistogram);
    }

    @Override
    public int hashCode() {
      return Objects.hash(runsCount, cardinalitySum, Arrays.hashCode(runLengthHistogram));
    }

    @Override
    public String toString() {
      return "RunContainersStats{"
          + "runsCount="
          + runsCount
          + ", runLengthHistogram="
          + Arrays.toString(runLengthHistogram)
          + ", cardinalitySum="
          + cardinalitySum
          + '}';
    }

    public static final RunContainersStats empty = new RunContainersStats(0, 0, new int[16]);
  }
}
