// https://github.com/RoaringBitmap/RoaringBitmap/issues/161
package org.roaringbitmap.equals;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.Container;
import org.roaringbitmap.RandomData;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RunContainer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Arrays;

@BenchmarkMode(AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class EqualsBenchmark {

  @Benchmark
  public boolean runVsArrayEquals_FewRuns(EqualsFewRunsBenchmarkState benchmarkState) {
    return benchmarkState.runContainer.equals(benchmarkState.arrayContainer);
  }

  @Benchmark
  public boolean runVsArrayEquals_ManyRuns(EqualsManyRunsBenchmarkState benchmarkState) {
    return benchmarkState.runContainer.equals(benchmarkState.arrayContainer);
  }

  @Benchmark
  public boolean arrayVsArrayEquals(EqualArrayContainersBenchmarkState state) {
    return state.left.equals(state.right);
  }

  @Benchmark
  public boolean equalBitmaps(EqualBitmapsState state) {
    return state.left.equals(state.right);
  }

  @State(Scope.Benchmark)
  public static class EqualsFewRunsBenchmarkState {

    Container runContainer = new RunContainer();
    Container arrayContainer = new ArrayContainer();

    public EqualsFewRunsBenchmarkState() {
      arrayContainer = addRange(arrayContainer, 20, 400);
      runContainer = addRange(runContainer, 20, 400);

      arrayContainer = addRange(arrayContainer, 501, 1500);
      runContainer = addRange(runContainer, 501, 1500);

      arrayContainer = addRange(arrayContainer, 3000, 4500);
      runContainer = addRange(runContainer, 3000, 4500);
    }
  }

  @State(Scope.Benchmark)
  public static class EqualBitmapsState {

    RoaringBitmap left;
    RoaringBitmap right;

    @Setup(Level.Trial)
    public void init() {
      left = RandomData.randomBitmap(50, 0.5, 0.5);
      right = left.clone();
    }
  }

  @State(Scope.Benchmark)
  public static class EqualArrayContainersBenchmarkState {
    @Param({"20", "200", "1000", "2000"})
    int size;

    @Param({"0.1", "0.5", "0.9", "1"})
    float firstMismatch;

    Container left;
    Container right;

    @Setup(Level.Trial)
    public void init() {
      char[] l = array(size);
      char[] r = Arrays.copyOf(l, size);
      int mismatch = (int) (firstMismatch * size);
      if (mismatch < size) {
        for (int i = 0; i < size; ++i) {
          r[i] += 4096;
        }
      }
      left = new ArrayContainer(size, l);
      right = new ArrayContainer(size, r);
    }
  }

  @State(Scope.Benchmark)
  public static class EqualsManyRunsBenchmarkState {

    Container runContainer = new RunContainer();
    Container arrayContainer = new ArrayContainer();

    public EqualsManyRunsBenchmarkState() {
      int cardinality = 0;
      int runLength = 20;
      int shift = 5;
      int runStart = 0;
      while (cardinality < 4096 - runLength) {
        cardinality += runLength;
        runContainer = addRange(runContainer, runStart, runStart + runLength);
        arrayContainer = addRange(arrayContainer, runStart, runStart + runLength);
        runStart += runLength + shift;
      }
    }
  }

  private static char[] array(int size) {
    if (size >= 4096) {
      throw new IllegalStateException();
    }
    char[] array = new char[size];
    for (int i = 0; i < size; ++i) {
      array[i] = (char) i;
    }
    return array;
  }

  static Container addRange(Container c, int min, int sup) {
    return c.iadd(min, sup);
  }
}
