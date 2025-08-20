package org.roaringbitmap.longlong;

import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, warmups = 0, jvmArgs = {"-Xms32g", "-Xmx32g"})
@Warmup(iterations = 2, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class AddRoaring64 {

  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({"10000", "100000", "1000000"})
    public int initialSize = 0;

    @Param({"10000", "100000", "1000000"})
    public int addedSize = 0;

    @Param({"true", "false"})
    public boolean orderedAdd = false;

    long[] addedIndexes;
    Roaring64Bitmap bitmapInitial;
    Roaring64Bitmap bitmapTest;

    @Setup(Level.Trial)
    public void setupTrial() {
      bitmapTest = bitmapInitial.clone();
    }
    public long[] baseSetup() {
      Random r = new Random(0L);
      Set<Long> indexSet = new HashSet<>();
      while (indexSet.size() < initialSize) {
        indexSet.add(r.nextLong());
      }
      long[] values = indexSet.stream().mapToLong(Long::longValue).sorted().toArray();
      bitmapInitial = Roaring64Bitmap.bitmapOf(values);
      return values;
    }
  }
  @State(Scope.Benchmark)
  public static class AddExistingState extends BenchmarkState {
    @Setup()
    public void setup() {
      if (addedSize > initialSize) {
        throw new IllegalStateException("addedSize must not be greater than initialSize");
      }
      long[] values = baseSetup();

      List<Long> list = new ArrayList<>();
      for (long value : values) {
        list.add(value);
      }

      if (addedSize < initialSize) {
        Collections.shuffle(list, new Random(1L));
      }
      addedIndexes = list.stream().mapToLong(Long::longValue).limit(addedSize).toArray();
      if (!orderedAdd) {
        Arrays.sort(addedIndexes);
      }

    }

    @State(Scope.Benchmark)
    public static class AddMissingState extends BenchmarkState {

      @Setup()
      public void setup() {
        long[] values = baseSetup();
        Random r = new Random(1L);

        Set<Long> indexSet = new HashSet<>();
        for (long index : values) {
          indexSet.add(index);
        }

        Set<Long> missedSet = new HashSet<>();
        while (missedSet.size() < addedSize) {
          long value = r.nextLong();
          if (!indexSet.contains(value))
            missedSet.add(r.nextLong());
        }

        if (orderedAdd) {
          addedIndexes = missedSet.stream().mapToLong(Long::longValue).sorted().toArray();
        } else {
          List<Long> list = new ArrayList<>(missedSet);
          Collections.shuffle(list, r);
          addedIndexes = list.stream().mapToLong(Long::longValue).toArray();
        }
      }
    }

    @Benchmark()
    public void addAllExisting(AddExistingState state) {
      Roaring64Bitmap bitmap = state.bitmapTest;
      bitmap.add(state.addedIndexes);
    }

    @Benchmark()
    public void addEachExisting(AddExistingState state) {
      Roaring64Bitmap bitmap = state.bitmapTest;
      for (long index : state.addedIndexes) {
        bitmap.addLong(index);
      }
    }

    @Benchmark()
    public void addAllMissing(AddMissingState state) {
      Roaring64Bitmap bitmap = state.bitmapTest;
      bitmap.add(state.addedIndexes);
    }

    @Benchmark()
    public void addEachMissing(AddMissingState state) {
      Roaring64Bitmap bitmap = state.bitmapTest;
      for (long index : state.addedIndexes) {
        bitmap.addLong(index);
      }
    }
  }
}
