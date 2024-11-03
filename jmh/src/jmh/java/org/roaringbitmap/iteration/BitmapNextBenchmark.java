package org.roaringbitmap.iteration;

import org.roaringbitmap.RoaringBitmap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(jvmArgsPrepend = "-XX:-TieredCompilation")
public class BitmapNextBenchmark {

  @Param({"0.1", "0.2", "0.3", "0.4", "0.5"})
  double density;

  final int size = 1000000;

  RoaringBitmap bitmap = new RoaringBitmap();
  BitSet bs = new BitSet();
  final int random_size = 1000;

  int[] random_array = new int[random_size];

  @Setup
  public void init() {
    long target_cardinality = Math.round(density * size);
    long actual_cardinality = 0;
    ThreadLocalRandom random = ThreadLocalRandom.current();
    while (actual_cardinality < target_cardinality) {
      int x = random.nextInt(size);
      actual_cardinality += bitmap.checkedAdd(x) ? 1 : 0;
      bs.set(x);
    }
    for (int k = 0; k < random_size; k++) {
      random_array[k] = random.nextInt(size);
    }
  }

  @Benchmark
  public long bitset_count() {
    long count = 0;
    for (int k = 0; k < random_size; k++) {
      count = count + (long) bs.nextSetBit(random_array[k]);
    }
    return count;
  }

  @Benchmark
  public long roaring_count() {
    long count = 0;
    for (int k = 0; k < random_size; k++) {
      count = count + (long) bitmap.nextValue(random_array[k]);
    }
    return count;
  }
}
