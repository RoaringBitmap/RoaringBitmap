package org.roaringbitmap.longlong;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, warmups = 0, jvmArgs = {"-Xms32g", "-Xmx32g"})
@Warmup(iterations = 2, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class FindRoaring64 {

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({"10000", "100000", "1000000"})
    public int size = 0;

    @Param({"true", "false"})
    public boolean ordered = false;

    long[] indexes;
    long[] missedIndexes;
    Roaring64Bitmap bitmap;

    @Setup()
    public void setup() {
      Random r = new Random(0L);
      Set<Long> indexSet = new HashSet<>();
      while (indexSet.size() < size) {
        indexSet.add(r.nextLong());
      }

      Set<Long> missedSet = new HashSet<>();
      while (missedSet.size() < size) {
        long value = r.nextLong();
        if (!indexSet.contains(value))
          missedSet.add(r.nextLong());
      }

      if (ordered) {
        indexes = indexSet.stream().mapToLong(Long::longValue).sorted().toArray();
        missedIndexes = missedSet.stream().mapToLong(Long::longValue).toArray();
      } else {
        List<Long> list = new ArrayList<>(indexSet);
        Collections.shuffle(list, r);
        indexes = list.stream().mapToLong(Long::longValue).toArray();

        list = new ArrayList<>(missedSet);
        Collections.shuffle(list, r);
        missedIndexes = list.stream().mapToLong(Long::longValue).toArray();
      }

      bitmap = new Roaring64Bitmap();
      for (long index : indexes) {
        bitmap.addLong(index);
      }

    }
  }
  @Benchmark()
  public void findPresent(BenchmarkState state, Blackhole blackhole) {
    Roaring64Bitmap bitmap = state.bitmap;
    for (long index : state.indexes) {
      blackhole.consume(bitmap.contains(index));
    }
  }
  @Benchmark()
  public void findMissing(BenchmarkState state, Blackhole blackhole) {
    Roaring64Bitmap bitmap = state.bitmap;
    for (long index : state.missedIndexes) {
      blackhole.consume(bitmap.contains(index));
    }
  }

}
