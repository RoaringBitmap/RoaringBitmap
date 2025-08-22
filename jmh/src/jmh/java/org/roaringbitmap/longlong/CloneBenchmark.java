package org.roaringbitmap.longlong;

import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, warmups = 0, jvmArgs = {"-Xms32g", "-Xmx32g"})
@Warmup(iterations = 2, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class CloneBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({"10", "100", "1000", "10000", "100000", "1000000"})
    public int size = 0;

    Roaring64Bitmap source;

    @Setup()
    public void setup() {
      Random r = new Random(0L);
      source = new Roaring64Bitmap();
      Set<Long> indexSet = new HashSet<>();
      while (indexSet.size() < size) {
        indexSet.add(r.nextLong());
      }
      source = Roaring64Bitmap.bitmapOf(indexSet.stream().mapToLong(Long::longValue).toArray());
    }
  }
  @Benchmark()
  public Roaring64Bitmap clone(BenchmarkState state) {
    return state.source.clone();
  }
}
