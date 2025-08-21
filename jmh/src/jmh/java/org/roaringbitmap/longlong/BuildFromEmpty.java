package org.roaringbitmap.longlong;

import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, warmups = 0, jvmArgs = {"-Xms32g", "-Xmx32g"})
@Warmup(iterations = 2, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BuildFromEmpty {

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({"10000", "100000", "1000000"})
    public int size = 0;

    @Param({"true", "false"})
    public boolean ordered = false;

    long[] indexes;

    @Setup()
    public void setup() {
      Random r = new Random(0L);
      Set<Long> indexSet = new HashSet<>();
      while (indexSet.size() < size) {
        indexSet.add(r.nextLong());
      }
      if (ordered) {
        indexes = indexSet.stream().mapToLong(Long::longValue).sorted().toArray();
      } else {
        List<Long> list = new ArrayList<>(indexSet);
        Collections.shuffle(list, r);
        indexes = list.stream().mapToLong(Long::longValue).toArray();
      }
    }
  }
  @Benchmark()
  public Roaring64Bitmap addLong(BenchmarkState state) {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    for (long index : state.indexes) {
      bitmap.addLong(index);
    }
    return bitmap;
  }
  
  @Benchmark()
  public Roaring64Bitmap bitmapOf(BenchmarkState state) {
    return Roaring64Bitmap.bitmapOf(state.indexes);
  }

  @Benchmark()
  public Roaring64Bitmap addArray(BenchmarkState state) {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.add(state.indexes);
    return bitmap;
  }

}
