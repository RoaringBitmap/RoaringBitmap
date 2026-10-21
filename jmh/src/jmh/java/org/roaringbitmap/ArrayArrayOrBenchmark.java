package org.roaringbitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class ArrayArrayOrBenchmark {

  @Param({"1", "16"})
  int containerCount;

  @Param({"1", "64", "256", "4096"})
  int rightCardinality;

  @Param({"0", "50", "100"})
  int overlapPercent;

  @Param({"false", "true"})
  boolean reverseOperands;

  private RoaringBitmap left;
  private RoaringBitmap right;

  @Setup(Level.Trial)
  public void setup() {
    int[] leftValues = spreadValues(ArrayContainer.DEFAULT_MAX_SIZE);
    int[] rightValues = rightValues(leftValues, rightCardinality, overlapPercent);
    RoaringBitmap first = new RoaringBitmap();
    RoaringBitmap second = new RoaringBitmap();
    for (int key = 0; key < containerCount; ++key) {
      int base = key << 16;
      for (int value : leftValues) {
        first.add(base | value);
      }
      for (int value : rightValues) {
        second.add(base | value);
      }
    }
    left = reverseOperands ? second : first;
    right = reverseOperands ? first : second;
    int leftCardinality = reverseOperands ? rightCardinality : ArrayContainer.DEFAULT_MAX_SIZE;
    int expectedRightCardinality =
        reverseOperands ? ArrayContainer.DEFAULT_MAX_SIZE : rightCardinality;
    requireArrayContainers(left, containerCount, leftCardinality);
    requireArrayContainers(right, containerCount, expectedRightCardinality);
  }

  @Benchmark
  public RoaringBitmap or() {
    return RoaringBitmap.or(left, right);
  }

  private static int[] spreadValues(int cardinality) {
    int[] values = new int[cardinality];
    for (int i = 0; i < cardinality; ++i) {
      values[i] = (int) ((long) i * BitmapContainer.MAX_CAPACITY / cardinality);
    }
    return values;
  }

  private static int[] rightValues(int[] leftValues, int cardinality, int overlapPercent) {
    boolean[] present = new boolean[BitmapContainer.MAX_CAPACITY];
    for (int value : leftValues) {
      present[value] = true;
    }
    int[] absentValues = new int[BitmapContainer.MAX_CAPACITY - leftValues.length];
    int absentIndex = 0;
    for (int value = 0; value < present.length; ++value) {
      if (!present[value]) {
        absentValues[absentIndex++] = value;
      }
    }

    int shared = cardinality * overlapPercent / 100;
    int[] values = new int[cardinality];
    for (int i = 0; i < shared; ++i) {
      int index =
          shared == leftValues.length
              ? i
              : (int) ((long) (i + 1) * leftValues.length / (shared + 1));
      values[i] = leftValues[index];
    }
    int distinct = cardinality - shared;
    for (int i = 0; i < distinct; ++i) {
      int index = (int) ((2L * i + 1) * absentValues.length / (2L * distinct));
      values[shared + i] = absentValues[index];
    }
    Arrays.sort(values);
    return values;
  }

  private static void requireArrayContainers(
      RoaringBitmap bitmap, int expectedCount, int expectedCardinalityPerContainer) {
    if (bitmap.highLowContainer.size() != expectedCount) {
      throw new IllegalStateException("Unexpected number of containers in benchmark input");
    }
    for (int i = 0; i < expectedCount; ++i) {
      Container container = bitmap.highLowContainer.getContainerAtIndex(i);
      if (!(container instanceof ArrayContainer)
          || container.getCardinality() != expectedCardinalityPerContainer) {
        throw new IllegalStateException("Benchmark inputs must contain matching ArrayContainers");
      }
    }
  }
}
