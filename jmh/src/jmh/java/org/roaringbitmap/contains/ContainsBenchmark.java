// https://github.com/RoaringBitmap/RoaringBitmap/pull/374
package org.roaringbitmap.contains;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

import org.roaringbitmap.*;

import org.openjdk.jmh.annotations.*;

@BenchmarkMode(AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class ContainsBenchmark {

  @Benchmark
  public boolean containsBitmaps(EqualBitmapsState state) {
    return state.left.contains(state.right);
  }

  @State(Scope.Benchmark)
  public static class EqualBitmapsState {

    RoaringBitmap left;
    RoaringBitmap right;

    @Setup(Level.Trial)
    public void init() {
      left = RandomData.randomBitmap(50, 0.5, 0.5);
      right = RandomData.randomBitmap(50, 0.5, 0.5);
    }
  }
}
